from scrapling import StealthyFetcher

url = "https://www.sge.com.cn/public/front/findArticleExtList?pageNo=1&pageSize=5&menuId=1738"

print(f"Fetching {url} with StealthyFetcher...")
try:
    resp = StealthyFetcher.fetch(url, headless=True, timeout=30000)
    print(f"Status: {resp.status}")
    print(f"Content Length: {len(resp.text)}")
    print(f"Preview: {resp.text[:200]}")
    
    # Try to parse as JSON if possible
    import json
    try:
        data = json.loads(resp.text)
        print("Successfully parsed JSON.")
        print("First item title:", data.get('list', [{}])[0].get('title'))
    except:
        print("Not a valid JSON response.")
except Exception as e:
    print(f"An error occurred: {e}")
