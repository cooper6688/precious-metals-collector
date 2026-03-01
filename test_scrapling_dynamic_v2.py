from scrapling import DynamicFetcher

url = "https://www.sge.com.cn/public/front/findArticleExtList?pageNo=1&pageSize=5&menuId=1738"

print(f"Fetching {url} with DynamicFetcher (google_search=False)...")
try:
    # Disable google_search and set a more neutral referer
    resp = DynamicFetcher.fetch(
        url, 
        headless=True, 
        timeout=30000, 
        google_search=False,
        extra_headers={"Referer": "https://www.sge.com.cn/sjzx/mrhq"}
    )
    print(f"Status: {resp.status}")
    print(f"Final URL: {resp.url}")
    print(f"Content Length: {len(resp.text)}")
    print(f"Preview: {resp.text[:200]}")
    
    import json
    try:
        data = json.loads(resp.text)
        print("Successfully parsed JSON.")
        print("First item title:", data.get('list', [{}])[0].get('title'))
    except:
        print("Not a valid JSON response.")
except Exception as e:
    print(f"An error occurred: {e}")
