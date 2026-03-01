from scrapling import DynamicFetcher

url = "https://www.sge.com.cn/public/front/findArticleExtList?pageNo=1&pageSize=5&menuId=1738"

print(f"Fetching {url} with DynamicFetcher...")
try:
    # Disable google_search redirect just in case
    resp = DynamicFetcher.fetch(url, headless=True, timeout=30000)
    print(f"Status: {resp.status}")
    print(f"URL: {resp.url}")
    print(f"Content Length: {len(resp.text)}")
    print(f"Preview: {resp.text[:200]}")
    
    import json
    try:
        data = json.loads(resp.text)
        print("Successfully parsed JSON.")
    except:
        print("Not a valid JSON response.")
except Exception as e:
    print(f"An error occurred: {e}")
