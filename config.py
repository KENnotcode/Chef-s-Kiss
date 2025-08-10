"""
Configuration settings for the TAAN web scraper
"""

import os

# Base URLs
BASE_URL = "https://www.taan.org.np"
MEMBERS_URLS = [
    f"{BASE_URL}/members",           # General Members (main)
    f"{BASE_URL}/associate-members", # Associate Members  
    f"{BASE_URL}/regional-members"   # Regional Association Members
]

# Scraping settings
MAX_WORKERS = 10  # Number of concurrent threads
REQUEST_TIMEOUT = 60  # Timeout for HTTP requests
RETRY_ATTEMPTS = 3  # Number of retry attempts for failed requests
DELAY_BETWEEN_REQUESTS = 0.5  # Minimum delay between requests (seconds)

# Output settings
def get_output_filename():
    base_name = "ScrapedData"
    extension = ".xlsx"
    
    if not os.path.exists(f"{base_name}{extension}"):
        return f"{base_name}{extension}"
    
    counter = 1
    while os.path.exists(f"{base_name}{counter}{extension}"):
        counter += 1
    
    return f"{base_name}{counter}{extension}"

OUTPUT_FILE = get_output_filename()
MISSING_DATA_PLACEHOLDER = "none"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# Data fields to extract
DATA_FIELDS = [
    'Organization Name',
    'Registration Number', 
    'VAT Number',
    'Address',
    'Country', 
    'Website URL',
    'Email',
    'Telephone Number',
    'Mobile Number',
    'Fax',
    'PO Box',
    'Key Person',
    'Establishment Date',
    'Member Type'
]

# Alphabetical filters for pagination
ALPHABET_FILTERS = [''] + [chr(i) for i in range(ord('a'), ord('z') + 1)]  # Empty string for trending page