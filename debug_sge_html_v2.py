from scrapling import StealthyFetcher

url = "https://www.sge.com.cn/sjzx/mrhq"

print(f"Fetching {url} and checking response properties (wait=5000)...")
try:
    resp = StealthyFetcher.fetch(url, headless=True, timeout=30000, wait=5000)
    print(f"Status: {resp.status}")
    print(f"resp.text length: {len(resp.text)}")
    print(f"resp.body length: {len(resp.body)}")
    print(f"resp.get() length: {len(resp.get())}")
    
    html = resp.get()
    with open("sge_debug_v2.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("Saved full HTML to sge_debug_v2.html")
    
    # Try a more robust selector for the links
    links = resp.css('a[href*="/sjzx/mrhq/"]::attr(href)').getall()
    print(f"Found {len(links)} article links using a[href*='/sjzx/mrhq/'].")
    for link in links[:5]:
        print(f" - {link}")

except Exception as e:
    print(f"Error: {e}")
