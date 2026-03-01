from scrapling import StealthyFetcher
import time

url = "https://www.sge.com.cn/sjzx/quotation_daily_new"

def click_search(page):
    print("Executing page_action: clicking search button...")
    try:
        # The search button usually has a specific class or text
        # On this page, it's often an <a> or <input> with "查询"
        # Let's wait for the page to load first
        page.wait_for_load_state("networkidle")
        
        # Check for the search button
        # Selector might be '.btn-search' or similar. 
        # Looking at SGE, it's often a .searchBtn or similar in the quotation page.
        search_button = page.locator("a:has-text('查询'), button:has-text('查询'), .searchBtn")
        if search_button.count() > 0:
            print("Clicking search button...")
            search_button.first.click()
            # Wait for the table
            page.wait_for_selector(".table-responsive table, .memberName table, #quotation_daily_new_data table", timeout=15000)
            print("Table found after clicking search!")
        else:
            print("Search button not found, maybe table loads automatically?")
            page.wait_for_selector("table", timeout=10000)
            print("A table was found.")
            
    except Exception as e:
        print(f"Error in page_action: {e}")

print(f"Fetching {url} and performing UI interaction...")
try:
    # Use headless=False for local debug if needed, but here we must stay headless for the agent
    resp = StealthyFetcher.fetch(url, headless=True, page_action=click_search, wait=3000)
    print(f"Status: {resp.status}")
    
    html = resp.get()
    with open("sge_ui_debug.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("Saved UI debug HTML.")
    
    # Check for content in the table
    rows = resp.css('table tr').getall()
    print(f"Found {len(rows)} rows in the final HTML.")
    for i, row in enumerate(resp.css('table tr')):
        if i < 10:
            cells = [c.strip() for c in row.css('td::text, th::text').getall() if c.strip()]
            if cells:
                print(f"Row {i}: {cells}")

except Exception as e:
    print(f"An error occurred: {e}")
