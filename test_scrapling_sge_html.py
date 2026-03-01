from scrapling import StealthyFetcher

url = "https://www.sge.com.cn/sjzx/mrhq"

print(f"Fetching {url} with StealthyFetcher...")
try:
    resp = StealthyFetcher.fetch(url, headless=True, timeout=30000)
    print(f"Status: {resp.status}")
    print(f"Final URL: {resp.url}")
    print(f"Title: {resp.css('title::text').get()}")
    
    # Check for some elements in the daily quotation page
    # SGE usually has a table or a list of links
    links = resp.css('div.list.list_1 a::attr(href)').getall()
    print(f"Found {len(links)} article links.")
    for link in links[:5]:
        print(f" - {link}")
        
except Exception as e:
    print(f"An error occurred: {e}")
