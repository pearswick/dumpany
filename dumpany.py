import os
import httpx
from datetime import datetime, timedelta
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
DOCS_DIR = os.getenv('DUMP_DIR', 'dump')

# Rate limiting settings
MAX_WORKERS = 5
RATE_WINDOW = 300  # 5 minutes in seconds

# Request tracking for stats and rate monitoring
REQUEST_TIMES = []
REQUEST_TIMES_LOCK = Lock()
REQUEST_COUNTER = {'total': 0}
REQUEST_COUNTER_LOCK = Lock()

# For main API (company info, filing history)
headers = {
    'Authorization': API_KEY
}


class RateLimiter:
    def __init__(self, max_requests=600, time_window=300):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []

    def wait_if_needed(self):
        now = datetime.now()
        self.requests = [t for t in self.requests if now - t < timedelta(seconds=self.time_window)]

        if len(self.requests) >= self.max_requests:
            oldest_request = self.requests[0]
            wait_time = self.time_window - (now - oldest_request).total_seconds()
            if wait_time > 0:
                console.print(f"[yellow]Rate limit approaching. Waiting {wait_time:.1f} seconds...[/]")
                time.sleep(wait_time)
                self.requests = []

        self.requests.append(now)

    def reset(self):
        self.requests = []


# Create a global rate limiter
rate_limiter = RateLimiter()


def count_request():
    current_time = time.time()
    with REQUEST_COUNTER_LOCK:
        REQUEST_COUNTER['total'] += 1

    with REQUEST_TIMES_LOCK:
        while REQUEST_TIMES and REQUEST_TIMES[0] < current_time - RATE_WINDOW:
            REQUEST_TIMES.pop(0)
        REQUEST_TIMES.append(current_time)


def get_current_rate():
    with REQUEST_TIMES_LOCK:
        current_time = time.time()
        while REQUEST_TIMES and REQUEST_TIMES[0] < current_time - RATE_WINDOW:
            REQUEST_TIMES.pop(0)
        return len(REQUEST_TIMES)


def get_company_details(company_number, debug=False):
    url = f"{BASE_API_URL}company/{company_number}"
    with httpx.Client() as client:
        count_request()
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
            count_request()
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
    metadata_link, save_path, company_name, description, debug = args
    max_retries = 3

    for attempt in range(max_retries):
        try:
            if debug:
                console.print(f"\n[blue]Attempt {attempt + 1}/{max_retries} for {description}")

            metadata_url = metadata_link.replace('/content', '')
            metadata = get_document_metadata(metadata_url, debug)
            if not metadata:
                raise Exception(f"Failed to get metadata from {metadata_url}")

            if debug:
                console.print(f"Metadata structure: {metadata.keys()}")

            document_url = metadata.get('links', {}).get('document')
            if not document_url:
                raise Exception(f"No document URL in metadata: {metadata}")

            rate_limiter.wait_if_needed()

            with httpx.Client() as client:
                response = client.get(document_url, auth=(API_KEY, ''), headers={'Accept': 'application/pdf'}, follow_redirects=False)

                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 300))
                    console.print(f"[yellow]Rate limited during download. Waiting {retry_after} seconds...[/]")
                    time.sleep(retry_after)
                    raise Exception("Rate limited")

                if response.status_code == 302:
                    redirect_url = response.headers['Location']
                    if debug:
                        print(f"Redirect URL: {redirect_url}")
                    response = client.get(redirect_url, headers={'Accept': 'application/pdf'})

                response.raise_for_status()
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                return True

        except Exception as e:
            if attempt < max_retries - 1:
                console.print(f"[yellow]Failed attempt {attempt + 1}/{max_retries} for {description}: {str(e)}[/]")
                time.sleep(2)
                continue
            else:
                console.print(f"[red]Failed to download {description}: {str(e)}[/]")
                return False

    return False


def dumpany(company_number, debug=False):
    company_details = get_company_details(company_number, debug)
    company_name = sanitize_filename(company_details.get('company_name', f'company_{company_number}'))

    output_dir = os.path.join(DOCS_DIR, company_name)
    os.makedirs(output_dir, exist_ok=True)

    filings = get_filing_history(company_number)
    pdf_filings = [f for f in filings if f.get('links', {}).get('document_metadata')]
    pdf_filings.sort(key=lambda f: datetime.strptime(f['date'], '%Y-%m-%d'))

    console.print(f"[green]Found {len(pdf_filings)} documents for {company_name}")

    download_tasks = []
    for filing in pdf_filings:
        metadata_link = filing['links']['document_metadata']
        created_date = filing['date']
        description = sanitize_filename(get_filing_description(filing))
        filename = f"{created_date}_{company_name}_{description}.pdf"
        save_path = os.path.join(output_dir, filename)

        if os.path.exists(save_path):
            console.print(f"[blue]Skipping existing file: {filename}")
            continue

        download_tasks.append((metadata_link, save_path, company_name, description, debug))

    if not download_tasks:
        console.print("[green]All documents already downloaded!")
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        download_task = progress.add_task(
            f"[green]Downloading documents for {company_name}...",
            total=len(download_tasks)
        )

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for result in executor.map(download_single_pdf, download_tasks):
                progress.update(download_task, advance=1)

    console.print(f"[bold green]✓[/] All available documents for {company_name} saved to {output_dir}")


def get_company_name(company_number):
    company_details = get_company_details(company_number)
    return company_details.get('company_name', f'company_{company_number}')


def sanitize_filename(name):
    name = ' '.join(name.split())
    name = name.replace('--', '-')
    name = name.replace('/', '_')
    name = name.replace('\\', '_')
    name = name.replace(':', '_')
    name = "".join(c for c in name if c.isalnum() or c in " ._-")

    while '  ' in name:
        name = name.replace('  ', ' ')
    while '--' in name:
        name = name.replace('--', '-')

    return name.strip()


def get_document_metadata(metadata_link, debug=False):
    rate_limiter.wait_if_needed()

    with httpx.Client() as client:
        if debug:
            print(f"\nFetching metadata from: {metadata_link}")

        try:
            response = client.get(metadata_link, auth=(API_KEY, ''))

            if debug:
                print(f"Metadata Status Code: {response.status_code}")
                print(f"Response: {response.text[:500]}")

            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 300))
                console.print(f"[yellow]Rate limited by API. Waiting {retry_after} seconds...[/]")
                rate_limiter.reset()
                time.sleep(retry_after)
                raise Exception("Rate limited")

            response.raise_for_status()
            return response.json()

        except Exception as e:
            if debug:
                print("Failed to get metadata: ", str(e))
            return None


def get_filing_description(filing):
    if filing['description'] in ['legacy', 'miscellaneous']:
        return filing.get('description_values', {}).get('description', 'unknown_document')
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

    console.print("[green]API key loaded successfully[/]")
    console.print(f"Documents will be saved to: {DOCS_DIR}")

    os.makedirs(DOCS_DIR, exist_ok=True)

    user_input = console.input("Enter company number(s) separated by commas: ").strip()
    company_numbers = [num.strip() for num in user_input.split(',')]

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

    current_rate = get_current_rate()
    console.print(f"\n[blue]Total API requests made: {REQUEST_COUNTER['total']}[/]")
    console.print(f"[blue]Current rate: {current_rate} requests in last 5 minutes[/]")
