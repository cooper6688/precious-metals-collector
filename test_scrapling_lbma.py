from scrapling import StealthyFetcher
import time

url = "https://www.lbma.org.uk/prices-and-data/precious-metal-prices"

def wait_for_lbma(page):
    print("Executing page_action: waiting for LBMA price table...")
    try:
        # Wait for potential cloudflare challenge to pass
        page.wait_for_load_state("networkidle", timeout=30000)
        # Wait for the price table
        page.wait_for_selector("table.metal-prices-table, .prices-table, table", timeout=20000)
        print("LBMA table detected!")
    except Exception as e:
        print(f"Warning: LBMA table not found or timeout: {e}")

print(f"Fetching {url} with solve_cloudflare=True...")
try:
    # LBMA is known for Cloudflare
    resp = StealthyFetcher.fetch(url, headless=True, page_action=wait_for_lbma, solve_cloudflare=True, wait=5000)
    print(f"Status: {resp.status}")
    print(f"Final URL: {resp.url}")
    
    html = resp.get()
    with open("lbma_debug.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("Saved LBMA debug HTML.")
    
    # Check for rows
    rows = resp.css('table tr').getall()
    print(f"Found {len(rows)} rows in the final HTML.")
    
    for i, row in enumerate(resp.css('table tr')[:10]):
        txt = [c.strip() for c in row.css('td::text, th::text').getall() if c.strip()]
        if txt:
            print(f"Row {i}: {txt}")

except Exception as e:
    print(f"An error occurred: {e}")
