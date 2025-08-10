"""
SCRAPEthis - Fast and accurate TAAN member data scraper
Scrapes all 2273 members from https://www.taan.org.np/members/
with concurrent processing and exports to Excel
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
import logging
from typing import List, Dict, Set
import threading
from collections import defaultdict
import os
from config import *
from scrapping_utils import *

class TAANScraper:
    def __init__(self):
        self.session = create_session()
        self.scraped_urls: Set[str] = set()
        self.scraped_data: List[Dict[str, str]] = []
        self.failed_urls: List[str] = []
        self.url_to_type: Dict[str, str] = {}
        self.lock = threading.Lock()
        self.progress_counter = 0
        self.start_time = None
        
        setup_logging()
        logging.info("SCRAPEthis initialized - TAAN Member Data Scraper")
        
    def get_all_member_urls(self) -> List[str]:
        """Get all member URLs from all member types and alphabetical pages"""
        logging.info("Starting to collect member URLs from all pages...")
        all_urls = []
        
        # Process each member type (General, Associate, Regional)
        url_to_type = {}  # Track which URLs belong to which member type
        
        for base_url in MEMBERS_URLS:
            member_type = base_url.split('/')[-1].replace('-', ' ').title()
            if member_type == "Members":
                member_type = "General"
            elif member_type == "Associate Members":
                member_type = "Associate"
            elif member_type == "Regional Members":
                member_type = "Regional"
            
            logging.info(f"Processing {member_type} Member pages...")
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {}
                
                # Submit tasks for each alphabetical filter
                for filter_char in ALPHABET_FILTERS:
                    if filter_char:
                        url = f"{base_url}?l={filter_char}"
                    else:
                        url = base_url  # Trending page
                    
                    future = executor.submit(self._get_page_member_urls, url)
                    futures[future] = f"{member_type}-{filter_char or 'trending'}"
                
                # Collect results
                for future in as_completed(futures):
                    filter_name = futures[future]
                    try:
                        urls = future.result()
                        if urls:  # Only log and add if URLs found
                            # Track member type for each URL
                            for url in urls:
                                url_to_type[url] = member_type
                            all_urls.extend(urls)
                            logging.info(f"Found {len(urls)} member URLs on '{filter_name}' page")
                    except Exception as e:
                        logging.error(f"Failed to get URLs from '{filter_name}' page: {str(e)}")
        
        # Store url_to_type mapping for later use
        self.url_to_type = url_to_type
        
        # Remove duplicates while preserving order
        unique_urls = []
        seen = set()
        for url in all_urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        logging.info(f"Total unique member URLs found: {len(unique_urls)}")
        return unique_urls
    
    def _get_page_member_urls(self, page_url: str) -> List[str]:
        """Get member URLs from a single page"""
        response = safe_request(self.session, page_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        member_urls = extract_member_urls(soup, BASE_URL)
        
        return member_urls
    
    def scrape_member_data(self, member_url: str, member_type: str = "General") -> Dict[str, str]:
        """Scrape data from a single member page"""
        try:
            # Check if already scraped (avoid duplicates)
            with self.lock:
                if member_url in self.scraped_urls:
                    return {}
                self.scraped_urls.add(member_url)
            
            response = safe_request(self.session, member_url)
            if not response:
                self.failed_urls.append(member_url)
                return {}
            
            soup = BeautifulSoup(response.content, 'html.parser')
            data = extract_member_data(soup)
            
            # Add member type
            data['Member Type'] = member_type
            
            # Validate data quality
            if not validate_data(data):
                logging.warning(f"Low quality data extracted from: {member_url}")
            
            # Update progress
            with self.lock:
                self.progress_counter += 1
                self._log_progress()
            
            return data
            
        except Exception as e:
            logging.error(f"Error scraping {member_url}: {str(e)}")
            self.failed_urls.append(member_url)
            return {}
    
    def _log_progress(self):
        """Log scraping progress"""
        if self.start_time:
            elapsed = time.time() - self.start_time
            rate = self.progress_counter / elapsed if elapsed > 0 else 0
            
            if self.progress_counter % 50 == 0 or self.progress_counter <= 10:
                logging.info(f"Progress: {self.progress_counter} members scraped | "
                           f"Rate: {rate:.2f} members/sec | "
                           f"Elapsed: {elapsed:.1f}s")
    
    def scrape_all_members(self) -> List[Dict[str, str]]:
        """Scrape all member data using concurrent processing"""
        logging.info("Starting concurrent member data scraping...")
        self.start_time = time.time()
        
        # Get all member URLs
        member_urls = self.get_all_member_urls()
        
        if not member_urls:
            logging.error("No member URLs found. Scraping aborted.")
            return []
        
        logging.info(f"Starting to scrape {len(member_urls)} members with {MAX_WORKERS} concurrent workers...")
        
        # Scrape with concurrent processing
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(self.scrape_member_data, url, self.url_to_type.get(url, "General")): url 
                for url in member_urls
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    if data:  # Only add non-empty data
                        with self.lock:
                            self.scraped_data.append(data)
                except Exception as e:
                    logging.error(f"Error processing {url}: {str(e)}")
                    self.failed_urls.append(url)
        
        total_time = time.time() - self.start_time
        success_rate = (len(self.scraped_data) / len(member_urls)) * 100 if member_urls else 0
        avg_rate = len(self.scraped_data) / total_time if total_time > 0 else 0
        
        logging.info(f"Scraping completed!")
        logging.info(f"Total members scraped: {len(self.scraped_data)}")
        logging.info(f"Failed URLs: {len(self.failed_urls)}")
        logging.info(f"Success rate: {success_rate:.1f}%")
        logging.info(f"Total time: {total_time:.1f} seconds")
        logging.info(f"Average rate: {avg_rate:.2f} members/second")
        
        return self.scraped_data
    
    def export_to_excel(self, data: List[Dict[str, str]], filename: str = OUTPUT_FILE):
        """Export scraped data to Excel file"""
        if not data:
            logging.error("No data to export")
            return
        
        try:
            # Create DataFrame with consistent columns
            df = pd.DataFrame(data, columns=DATA_FIELDS)
            
            # Fill any remaining NaN values with placeholder
            df = df.fillna(MISSING_DATA_PLACEHOLDER)
            
            # Save to Excel
            df.to_excel(filename, index=False, engine='openpyxl')
            
            logging.info(f"Data exported successfully to {filename}")
            logging.info(f"Exported {len(df)} rows and {len(df.columns)} columns")
            
            # Log summary statistics
            self._log_data_summary(df)
            
        except Exception as e:
            logging.error(f"Error exporting to Excel: {str(e)}")
    
    def _log_data_summary(self, df: pd.DataFrame):
        """Log summary of scraped data quality"""
        logging.info("=== Data Quality Summary ===")
        
        total_cells = len(df) * len(df.columns)
        empty_cells = (df == MISSING_DATA_PLACEHOLDER).sum().sum()
        filled_rate = ((total_cells - empty_cells) / total_cells) * 100
        
        logging.info(f"Total data points: {total_cells}")
        logging.info(f"Filled data points: {total_cells - empty_cells}")
        logging.info(f"Data fill rate: {filled_rate:.1f}%")
        
        # Log fill rates by field
        for column in df.columns:
            filled = (df[column] != MISSING_DATA_PLACEHOLDER).sum()
            rate = (filled / len(df)) * 100
            logging.info(f"{column}: {filled}/{len(df)} ({rate:.1f}%)")
    
    def retry_failed_urls(self):
        """Retry scraping failed URLs"""
        if not self.failed_urls:
            logging.info("No failed URLs to retry")
            return
        
        logging.info(f"Retrying {len(self.failed_urls)} failed URLs...")
        failed_copy = self.failed_urls.copy()
        self.failed_urls.clear()
        
        retry_data = []
        for url in failed_copy:
            member_type = self.url_to_type.get(url, "General")
            data = self.scrape_member_data(url, member_type)
            if data:
                retry_data.append(data)
        
        if retry_data:
            self.scraped_data.extend(retry_data)
            logging.info(f"Successfully recovered {len(retry_data)} members on retry")
        
        return retry_data

def main():
    """Main function to run the TAAN scraper"""
    print("=" * 60)
    print("SCRAPEthis - TAAN Member Data Scraper")
    print("Fast and accurate scraping with concurrent processing")
    print("=" * 60)
    
    scraper = TAANScraper()
    
    try:
        # Scrape all member data
        scraped_data = scraper.scrape_all_members()
        
        # Retry failed URLs once
        scraper.retry_failed_urls()
        
        if scraped_data:
            # Export to Excel
            scraper.export_to_excel(scraped_data)
            
            print("\n" + "=" * 60)
            print("SCRAPING COMPLETED SUCCESSFULLY!")
            print(f"Total members scraped: {len(scraped_data)}")
            print(f"Data exported to: {OUTPUT_FILE}")
            print(f"Failed URLs: {len(scraper.failed_urls)}")
            
            if scraper.failed_urls:
                print("\nFailed URLs (logged in scraper.log):")
                for url in scraper.failed_urls[:5]:  # Show first 5
                    print(f"  - {url}")
                if len(scraper.failed_urls) > 5:
                    print(f"  ... and {len(scraper.failed_urls) - 5} more")
            
            print("=" * 60)
            
        else:
            print("No data was scraped. Please check the logs for errors.")
            
    except KeyboardInterrupt:
        logging.info("Scraping interrupted by user")
        print("\nScraping interrupted by user.")
        
        # Export any data that was scraped before interruption
        if scraper.scraped_data:
            scraper.export_to_excel(scraper.scraped_data, f"partial_{OUTPUT_FILE}")
            print(f"Partial data exported to: partial_{OUTPUT_FILE}")
            
    except Exception as e:
        logging.error(f"Critical error in main: {str(e)}")
        print(f"Critical error: {str(e)}")

if __name__ == "__main__":
    main()
