# scrapping_utils.py
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import logging
from typing import Dict, List, Optional
from config import *

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('scraper.log'),
            logging.StreamHandler()
        ]
    )

def create_session() -> requests.Session:
    """Create a requests session with proper headers and settings"""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session

def safe_request(session: requests.Session, url: str, max_retries: int = RETRY_ATTEMPTS) -> Optional[requests.Response]:
    """Make a safe HTTP request with retry logic"""
    for attempt in range(max_retries):
        try:
            time.sleep(DELAY_BETWEEN_REQUESTS)
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {url} - {str(e)}")
            if attempt == max_retries - 1:
                logging.error(f"Failed to fetch {url} after {max_retries} attempts")
                return None
            time.sleep(2 ** attempt)  # Exponential backoff
    return None

def extract_member_urls(soup: BeautifulSoup, base_url: str) -> List[str]:
    """Extract member detail URLs from listing page"""
    member_urls = []
    
    # Find all member links - they appear to be in anchor tags with specific patterns
    links = soup.find_all('a', href=lambda x: x and '/members/' in x and x != '/members/')
    
    for link in links:
        href = link.get('href')
        if href and '/members/' in href:
            # Convert relative URLs to absolute
            full_url = urljoin(base_url, href)
            if full_url not in member_urls:
                member_urls.append(full_url)
    
    return member_urls

def clean_text(text: str) -> str:
    """Clean and normalize text data"""
    if not text:
        return MISSING_DATA_PLACEHOLDER
    
    # Remove extra whitespace and newlines
    cleaned = ' '.join(text.strip().split())
    
    # Return placeholder if empty after cleaning
    return cleaned if cleaned else MISSING_DATA_PLACEHOLDER

def extract_member_data(soup: BeautifulSoup) -> Dict[str, str]:
    """Extract member data from individual member page"""
    data = {}
    
    try:
        # Initialize all fields with placeholder
        for field in DATA_FIELDS:
            data[field] = MISSING_DATA_PLACEHOLDER
        
        # Extract organization name from title or heading
        title_elem = soup.find('h1') or soup.find('title')
        if title_elem:
            org_name = clean_text(title_elem.get_text())
            if org_name != MISSING_DATA_PLACEHOLDER:
                data['Organization Name'] = org_name
        
        # Extract data from the structured list items
        # Look for patterns like "Reg. No:", "Address:", etc.
        list_items = soup.find_all(['li', 'div', 'p', 'td'])
        
        for item in list_items:
            text = item.get_text().strip()
            if ':' in text:
                parts = text.split(':', 1)
                if len(parts) == 2:
                    key = parts[0].strip().lower()
                    value = clean_text(parts[1])
                    
                    # Map keys to our standard field names
                    if 'organization name' in key:
                        data['Organization Name'] = value
                    elif 'reg' in key and 'no' in key:
                        data['Registration Number'] = value
                    elif 'vat' in key:
                        data['VAT Number'] = value
                    elif 'address' in key:
                        data['Address'] = value
                    elif 'country' in key:
                        data['Country'] = value
                    elif 'website' in key or 'url' in key:
                        # Extract actual URL from link if present
                        link = item.find('a')
                        if link and link.get('href'):
                            data['Website URL'] = link.get('href')
                        else:
                            data['Website URL'] = value
                    elif 'email' in key:
                        # Extract email from mailto link if present
                        link = item.find('a')
                        if link and link.get('href') and 'mailto:' in link.get('href'):
                            data['Email'] = link.get('href').replace('mailto:', '')
                        else:
                            data['Email'] = value
                    elif 'telephone' in key:
                        data['Telephone Number'] = value
                    elif 'mobile' in key:
                        data['Mobile Number'] = value
                    elif 'fax' in key:
                        data['Fax'] = value
                    elif 'po box' in key or 'p.o' in key:
                        data['PO Box'] = value
                    elif 'key person' in key:
                        data['Key Person'] = value
                    elif 'establishment' in key or 'date' in key:
                        data['Establishment Date'] = value
        
        # Also check for table format (Official Docs section)
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    key = cells[0].get_text().strip().lower().replace(':', '')
                    value = clean_text(cells[1].get_text())
                    
                    # Map table keys to our fields
                    if 'organization name' in key:
                        data['Organization Name'] = value
                    elif 'reg' in key:
                        data['Registration Number'] = value
                    elif 'address' in key:
                        data['Address'] = value
                    elif 'country' in key:
                        data['Country'] = value
                    elif 'website' in key:
                        link = cells[1].find('a')
                        if link and link.get('href'):
                            data['Website URL'] = link.get('href')
                        else:
                            data['Website URL'] = value
                    elif 'email' in key:
                        link = cells[1].find('a')
                        if link and link.get('href') and 'mailto:' in link.get('href'):
                            data['Email'] = link.get('href').replace('mailto:', '')
                        else:
                            data['Email'] = value
                    elif 'telephone' in key:
                        data['Telephone Number'] = value
                    elif 'mobile' in key:
                        data['Mobile Number'] = value
                    elif 'p.o' in key or 'po box' in key:
                        data['PO Box'] = value
                    elif 'key person' in key:
                        data['Key Person'] = value
        
    except Exception as e:
        logging.error(f"Error extracting member data: {str(e)}")
    
    return data

def validate_data(data: Dict[str, str]) -> bool:
    """Validate extracted data quality"""
    # Check if we have at least an organization name
    org_name = data.get('Organization Name', MISSING_DATA_PLACEHOLDER)
    if org_name == MISSING_DATA_PLACEHOLDER:
        return False
    
    # Count non-empty fields
    non_empty_fields = sum(1 for value in data.values() if value != MISSING_DATA_PLACEHOLDER)
    
    # Require at least 3 fields to be populated for valid data
    return non_empty_fields >= 3