import os
import httpx
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time
import argparse
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich import print as rprint

# Initialize Rich console
console = Console()

# Load environment variables from .env file
load_dotenv()

# Base URLs and endpoints
BASE_API_URL = "https://api.companieshouse.gov.uk/"
DOCUMENT_API_URL = "https://document-api.company-information.service.gov.uk/"

# Get environment variables
API_KEY = os.getenv('API_KEY')
DOCS_DIR = os.getenv('DUMP_DIR', 'dump')  # Changed from DOCS_DIR to DUMP_DIR with default 'dump'

# Rate limiting settings
MAX_WORKERS = 5  # Maximum concurrent downloads
REQUEST_LOCK = Lock()
LAST_REQUEST_TIME = {}  # Dictionary to track request times per domain
MIN_REQUEST_INTERVAL = 0.5  # Minimum time between requests to same domain (seconds)

if not API_KEY:
    console.print("[red]Error: API key not found in .env file[/]")
    exit(1)

console.print("[green]API key loaded successfully[/]")
console.print(f"Documents will be saved to: {DOCS_DIR}")

# Create docs directory if it doesn't exist
os.makedirs(DOCS_DIR, exist_ok=True)

# For main API (company info, filing history)
headers = {
    'Authorization': API_KEY
}

def rate_limited_request(domain):
    with REQUEST_LOCK:
        current_time = time.time()
        if domain in LAST_REQUEST_TIME:
            time_since_last = current_time - LAST_REQUEST_TIME[domain]
            if time_since_last < MIN_REQUEST_INTERVAL:
                time.sleep(MIN_REQUEST_INTERVAL - time_since_last)
        LAST_REQUEST_TIME[domain] = time.time()

def get_company_details(company_number, debug=False):
    url = f"{BASE_API_URL}company/{company_number}"
    with httpx.Client() as client:
        # Debug: Print request headers only if debug mode is enabled
        if debug:
            request = client.build_request("GET", url, headers=headers)
            print("\nDebug - Request Headers:")
            print(f"URL: {url}")
            print(f"Authorization: {request.headers.get('Authorization')}")
            print("All Headers:", request.headers)
            
            response = client.get(url, headers=headers)
            print("\nDebug - Response:")
            print(f"Status Code: {response.status_code}")
            print(f"Response Headers: {response.headers}")
        else:
            response = client.get(url, headers=headers)
        
        response.raise_for_status()
        return response.json()

def get_filing_history(company_number):
    filings = []
    page = 1
    with httpx.Client() as client:
        while True:
            url = f"{BASE_API_URL}company/{company_number}/filing-history"
            params = {'items_per_page': 100, 'start_index': (page - 1) * 100}
            response = client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            filings.extend(data.get('items', []))
            if len(data.get('items', [])) < 100:
                break
            page += 1
    return filings

def download_single_pdf(args):
    pdf_url, save_path, company_name, description, debug = args
    try:
        # Rate limit requests to each domain
        domain = pdf_url.split('/')[2]
        rate_limited_request(domain)
        
        # Download the PDF
        ch_headers = {'Accept': 'application/pdf'}
        
        with httpx.Client() as client:
            # Debug the request
            if debug:
                print(f"\nTrying to fetch PDF:")
                print(f"URL: {pdf_url}")
            
            # First request to Companies House with auth
            response = client.get(pdf_url, auth=(API_KEY, ''), headers=ch_headers, follow_redirects=False)
            if debug:
                print(f"Initial Status Code: {response.status_code}")
            
            if response.status_code == 302:
                redirect_url = response.headers['Location']
                if debug:
                    print(f"Redirect URL: {redirect_url}")
                # Second request to S3 with ONLY Accept header (no auth)
                s3_headers = {'Accept': 'application/pdf'}
                response = client.get(redirect_url, headers=s3_headers)
            
            response.raise_for_status()
            with open(save_path, 'wb') as f:
                f.write(response.content)
            return True
    except Exception as e:
        print(f"Failed to download {description}: {str(e)}")
        return False

def dumpany(company_number, debug=False):
    # Get company details
    company_details = get_company_details(company_number, debug)
    company_name = sanitize_filename(company_details.get('company_name', f'company_{company_number}'))
    
    # Create company directory
    output_dir = os.path.join(DOCS_DIR, company_name)
    os.makedirs(output_dir, exist_ok=True)
    
    # Get filing history
    filings = get_filing_history(company_number)
    pdf_filings = [f for f in filings if f.get('links', {}).get('document_metadata')]
    pdf_filings.sort(key=lambda f: datetime.strptime(f['date'], '%Y-%m-%d'))
    
    console.print(f"[green]Found {len(pdf_filings)} documents for {company_name}")
    
    # Prepare download tasks with debug flag
    download_tasks = []
    
    # Use a single Progress instance for both metadata and downloads
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        # Add both tasks at the start
        metadata_task = progress.add_task(
            f"[blue]Fetching metadata for {company_name}...",
            total=len(pdf_filings)
        )
        download_task = progress.add_task(
            f"[green]Downloading documents for {company_name}...",
            total=len(pdf_filings),
            visible=False  # Hide until metadata is complete
        )
        
        # First phase: Fetch metadata
        for filing in pdf_filings:
            metadata_link = filing['links']['document_metadata']
            metadata = get_document_metadata(metadata_link, debug)
            if metadata and metadata.get('links', {}).get('document'):
                created_at = metadata.get('created_at', filing['date'])
                created_date = created_at.split('T')[0] if 'T' in created_at else created_at
                description = get_filing_description(filing)
                description = sanitize_filename(description)
                filename = f"{created_date}_{company_name}_{description}.pdf"
                save_path = os.path.join(output_dir, filename)
                download_tasks.append((
                    metadata['links']['document'],
                    save_path,
                    company_name,
                    description,
                    debug  # Add debug flag to task tuple
                ))
            progress.update(metadata_task, advance=1)
        
        # Hide metadata task and show download task
        progress.update(metadata_task, visible=False)
        progress.update(download_task, visible=True)
        
        # Second phase: Download PDFs
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for result in executor.map(download_single_pdf, download_tasks):
                progress.update(download_task, advance=1)
    
    console.print(f"[bold green]✓[/] All available documents for {company_name} saved to {output_dir}")

def get_company_name(company_number):
    company_details = get_company_details(company_number)
    return company_details.get('company_name', f'company_{company_number}')

def sanitize_filename(name):
    # Replace multiple spaces/dashes with a single space
    name = ' '.join(name.split())
    name = name.replace('--', '-')
    
    # Remove or replace problematic characters
    name = name.replace('/', '_')
    name = name.replace('\\', '_')
    name = name.replace(':', '_')
    
    # Remove any characters not safe for filenames
    name = "".join(c for c in name if c.isalnum() or c in " ._-")
    
    # Clean up any remaining multiple spaces or dashes
    while '  ' in name:
        name = name.replace('  ', ' ')
    while '--' in name:
        name = name.replace('--', '-')
    
    return name.strip()

def get_document_metadata(metadata_link, debug=False):
    with httpx.Client(auth=(API_KEY, '')) as client:
        # Only show debug info if debug mode is enabled
        if debug:
            print(f"\nTrying metadata request:")
            print(f"URL: {metadata_link}")
        
        response = client.get(metadata_link)
        
        if debug:
            print(f"Status Code: {response.status_code}")
        
        if response.status_code != 200:
            if debug:
                print(f"Failed to get metadata: {response.text}")
            return None
            
        return response.json()

def get_filing_description(filing):
    # For legacy or miscellaneous filings, use the description from description_values
    if filing['description'] in ['legacy', 'miscellaneous']:
        return filing.get('description_values', {}).get('description', 'unknown_document')
    
    # Otherwise use the standard description
    return filing.get('description', 'unknown_document')

def show_intro():
    intro_art = """
                                   █████                                                                     
                                  ░░███                                                                      
                                ███████  █████ ████ █████████████   ████████   ██████   ████████   █████ ████
                               ███░░███ ░░███ ░███ ░░███░░███░░███ ░░███░░███ ░░░░░███ ░░███░░███ ░░███ ░███ 
                              ░███ ░███  ░███ ░███  ░███ ░███ ░███  ░███ ░███  ███████  ░███ ░███  ░███ ░███ 
                              ░███ ░███  ░███ ░███  ░███ ░███ ░███  ░███ ░███ ███░░███  ░███ ░███  ░███ ░███ 
                              ░░████████ ░░████████ █████░███ █████ ░███████ ░░████████ ████ █████ ░░███████ 
                               ░░░░░░░░   ░░░░░░░░ ░░░░░ ░░░ ░░░░░  ░███░░░   ░░░░░░░░ ░░░░ ░░░░░   ░░░░░███ 
                                                                    ░███                            ███ ░███ 
                                                                    █████                          ░░██████  
                                                                   ░░░░░                            ░░░░░░   
    """
    console.print(f"[bold medium_spring_green]{intro_art}[/]")
    console.print("\n[bold dim]📄 dumpany (c) 2025 pearswick[/]")
    console.print("[dim]A document downloader for UK companies[/]\n")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download Companies House documents')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    args = parser.parse_args()
    
    show_intro()
    
    if not API_KEY:
        console.print("[red]Error: API_KEY not found in environment variables[/]")
        exit(1)
    
    user_input = console.input("Enter company number(s) separated by commas: ").strip()
    company_numbers = [num.strip() for num in user_input.split(',')]
    
    # Get and display company names
    companies = []
    for number in company_numbers:
        try:
            name = get_company_name(number)
            companies.append(f"• {name} ({number})")
        except Exception as e:
            console.print(f"[red]Error fetching company {number}: {str(e)}[/]")
            continue
    
    if not companies:
        console.print("[red]No valid companies found.[/]")
        exit(1)
    
    console.print("\nDumpany will download all available PDFs for these companies:")
    for company in companies:
        console.print(f"[blue]{company}[/]")
    
    proceed = console.input("\nProceed? (y/n): ").lower().strip()
    if proceed != 'y':
        console.print("[yellow]Operation cancelled.[/]")
        exit(0)
    
    for number in company_numbers:
        try:
            dumpany(number, args.debug)
        except Exception as e:
            console.print(f"[red]Error processing company {number}: {str(e)}[/]")
