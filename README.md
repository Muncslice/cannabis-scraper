# Cannabis Store Scraper

A Python web scraper for extracting cannabis store information from AskMaryJ.com.

## Features
- Scrapes store name, address, phone number, and social media links
- Handles pagination across multiple pages
- Bypasses anti-bot protection using rotating proxies and headless browsers
- Saves data in both JSON and CSV formats
- Implements exponential backoff for retries

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/cannabis-scraper.git
cd cannabis-scraper
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Install Playwright browsers:
```bash
playwright install
```

## Usage

Run the scraper:
```bash
python cannabis_scraper.py
```

The script will:
1. Scrape all store listings from AskMaryJ.com
2. Save the results in both JSON and CSV formats
3. Log progress to scraper.log

## Configuration

You can modify the following settings in `cannabis_scraper.py`:
- `PROXIES`: List of proxy servers to use
- `USER_AGENTS`: List of user agent strings
- `BASE_URL`: The target website URL
- `TIMEOUT`: Request timeout in seconds

## Contributing

1. Fork the repository
2. Create a new branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Create a new Pull Request

## License

MIT License - See LICENSE file for details
