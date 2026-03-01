from scrapling import StealthyFetcher
import time

url = "https://www.sge.com.cn/sjzx/mrhq"

def wait_for_table(page):
    print("Executing page_action: waiting for #inshqinfo table...")
    try:
        # Wait for the table to be loaded via AJAX
        page.wait_for_selector("#inshqinfo table", timeout=15000)
        print("Table detected!")
        # Give it a tiny bit of extra time just in case
        page.wait_for_timeout(1000)
    except Exception as e:
        print(f"Warning: Table not found or timeout: {e}")

print(f"Fetching {url} and waiting for AJAX content...")
try:
    # We use wait_for_table action
    resp = StealthyFetcher.fetch(url, headless=True, page_action=wait_for_table, wait=2000)
    print(f"Status: {resp.status}")
    
    # Check if the table is in the response HTML
    html = resp.get()
    if "<table" in html and "inshqinfo" in html:
        print("Success! Table found in the rendered HTML.")
        # Try to extract some data
        rows = resp.css('#inshqinfo table tr').getall()
        print(f"Found {len(rows)} rows in the table.")
        
        # Save for inspection
        with open("sge_table_debug.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("Saved rendered HTML to sge_table_debug.html")
        
        # Print the first few rows text
        for i, row in enumerate(resp.css('#inshqinfo table tr')):
            if i < 5:
                # Get text from cells
                cells = row.css('td::text').getall()
                print(f"Row {i}: {cells}")
    else:
        print("Table still not found in the final HTML.")
        with open("sge_failed_debug.html", "w", encoding="utf-8") as f:
            f.write(html)

except Exception as e:
    print(f"An error occurred: {e}")
