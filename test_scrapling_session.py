from scrapling.fetchers import StealthySession

url_main = "https://www.sge.com.cn/sjzx/mrhq"
url_json = "https://www.sge.com.cn/public/front/findArticleExtList?pageNo=1&pageSize=5&menuId=1738"

print("Starting StealthySession...")
try:
    with StealthySession(headless=True, timeout=30000) as session:
        print(f"Fetching main page to establish session: {url_main}")
        resp1 = session.fetch(url_main, google_search=False)
        print(f"Main Page Status: {resp1.status}")
        
        print(f"Fetching JSON API: {url_json}")
        # Use the same session, referer should be the main page
        resp2 = session.fetch(url_json, google_search=False, extra_headers={"Referer": url_main})
        print(f"JSON Status: {resp2.status}")
        print(f"JSON Final URL: {resp2.url}")
        print(f"Body length: {len(resp2.body)}")
        
        if resp2.status == 200 and len(resp2.body) > 0:
            print("Response body preview:", resp2.body[:200])
            import json
            try:
                data = json.loads(resp2.body.decode('utf-8'))
                print("Successfully parsed JSON!")
                for item in data.get('list', []):
                    print(f" - {item.get('title')}")
            except Exception as je:
                print(f"JSON parse error: {je}")
        else:
            print("Failed to get a valid response for JSON.")
            
except Exception as e:
    print(f"An error occurred: {e}")
