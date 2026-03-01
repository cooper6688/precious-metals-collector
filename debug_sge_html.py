from scrapling import StealthyFetcher

url = "https://www.sge.com.cn/sjzx/mrhq"

print(f"Fetching {url} and saving to sge_debug.html (wait=5000)...")
try:
    resp = StealthyFetcher.fetch(url, headless=True, timeout=30000, wait=5000)
    print(f"Status: {resp.status}")
    print(f"Text length: {len(resp.text)}")
    with open("sge_debug.html", "w", encoding="utf-8") as f:
        f.write(resp.text)
    print("Saved to sge_debug.html")
except Exception as e:
    print(f"Error: {e}")
