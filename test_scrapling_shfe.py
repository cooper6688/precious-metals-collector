from scrapling import StealthyFetcher
import time

url = "https://www.shfe.com.cn/statements/dataview.html?paramid=kx"

def wait_for_shfe(page):
    print("Executing page_action: waiting for SHFE data table...")
    try:
        page.wait_for_load_state("networkidle")
        # SHFE uses complex JS grids (often MiniUI or similar)
        # We wait for a table or a specific div that contains data
        page.wait_for_selector("table, .mini-grid-table", timeout=20000)
        print("SHFE data container detected!")
        # Extra wait for the grid to populate
        page.wait_for_timeout(2000)
    except Exception as e:
        print(f"Warning: SHFE table not found or timeout: {e}")

print(f"Fetching {url}...")
try:
    resp = StealthyFetcher.fetch(url, headless=True, page_action=wait_for_shfe, wait=3000)
    print(f"Status: {resp.status}")
    
    html = resp.get()
    with open("shfe_debug.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("Saved SHFE debug HTML.")
    
    # Check for rows
    # We might need to be more specific with the selector if it's a grid
    rows = resp.css('table tr').getall()
    print(f"Found {len(rows)} rows in the final HTML.")
    
    # If the rows are 0, maybe it's divs?
    if len(rows) == 0:
        cells = resp.css('.mini-grid-cell-inner::text').getall()
        print(f"Found {len(cells)} grid cells.")
        for i, cell in enumerate(cells[:20]):
            print(f" Cell {i}: {cell.strip()}")
    else:
        for i, row in enumerate(resp.css('table tr')[:10]):
            txt = [c.strip() for c in row.css('td::text, th::text').getall() if c.strip()]
            if txt:
                print(f"Row {i}: {txt}")

except Exception as e:
    print(f"An error occurred: {e}")
