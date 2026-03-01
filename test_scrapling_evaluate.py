from scrapling import StealthyFetcher
import json

url_main = "https://www.sge.com.cn/sjzx/mrhq"
url_json = "https://www.sge.com.cn/public/front/findArticleExtList?pageNo=1&pageSize=5&menuId=1738"

def my_action(page):
    print("Executing page_action: fetching JSON via evaluate (Async IIFE)...")
    # Perform fetch inside the browser using an async IIFE
    js_code = f"""
    (async () => {{
        try {{
            const response = await fetch('{url_json}');
            const text = await response.text();
            return text;
        }} catch (err) {{
            return 'Error: ' + err.toString();
        }}
    }})()
    """
    # page.evaluate will wait for the promise to resolve if we use an async IIFE
    data = page.evaluate(js_code)
    print("Data received from evaluate!")
    if data and isinstance(data, str) and not data.startswith("Error:"):
        print("Preview:", data[:200])
        try:
            parsed = json.loads(data)
            print("Successfully parsed JSON inside page_action!")
            if 'list' in parsed:
                for item in parsed.get('list', []):
                    print(f" - {item.get('title')}")
            else:
                print("JSON structure unexpected (no 'list' field):", data[:500])
        except Exception as je:
            print(f"JSON parse error: {je}")
            print("Raw data causing error:", data[:500])
    else:
        print(f"Fetch failed or returned error: {data}")
    return data

print(f"Fetching {url_main} and executing internal fetch...")
try:
    resp = StealthyFetcher.fetch(url_main, headless=True, page_action=my_action)
    print(f"Main Page Fetch completed with status {resp.status}")

except Exception as e:
    print(f"An error occurred: {e}")
