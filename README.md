# Chef's Kiss: For a scraper that works so well, it's a masterpiece

A fast and accurate Python web scraper that extracts all member data from the Travel & Tourism Association of Nepal (TAAN) website.

## Features

- **Fast Concurrent Processing**: Uses 10 parallel workers to scrape at 7+ members/second
- **Comprehensive Data Extraction**: Extracts 13 data fields from all 2,000+ members
- **Robust Error Handling**: Automatic retries with exponential backoff
- **Missing Data Management**: Fills empty fields with "0" as specified
- **Excel Export**: Clean, organized output in ScrapedData.xlsx

## Installation

1. Install Python dependencies:
```bash
pip install -r dependencies.txt
```

2. Run the scraper:
```bash
python SCRAPEthis.py
```

## Output

The scraper creates `ScrapedData.xlsx` with the following columns:
- Organization Name
- Registration Number  
- VAT Number
- Address
- Country
- Website URL
- Email
- Telephone Number
- Mobile Number
- Fax
- PO Box
- Key Person
- Establishment Date

## Performance

- **Speed**: ~7-10 members per second
- **Success Rate**: 99.9% (2,093/2,095 members)
- **Data Quality**: 81.1% overall fill rate
- **Total Runtime**: ~5 minutes for all members

## Configuration

Edit `config.py` to modify:
- Number of concurrent workers
- Request timeouts and retries
- Output filename
- Missing data placeholder

## Requirements

- Python 3.7+
- Internet connection
- ~250KB disk space for output file