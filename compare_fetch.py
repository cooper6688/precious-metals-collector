import json
from curl_cffi import requests
from scrapling import Fetcher

sge_url = "https://www.sge.com.cn/public/front/findArticleExtList?pageNo=1&pageSize=15&menuId=1738"
ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

def test_curl_cffi():
    print("Testing SGE with curl_cffi...")
    resp = requests.get(sge_url, impersonate="chrome110", headers={"User-Agent": ua}, timeout=15)
    print(f"Status: {resp.status_code}")
    print(f"Content Length: {len(resp.content)}")
    print(f"Preview: {resp.text[:200]}")
    try:
        data = resp.json()
        print(f"JSON Keys: {data.keys()}")
    except:
        print("Not JSON")

def test_scrapling_no_stealth():
    from scrapling.engines.static import FetcherClient
    print("\nTesting SGE with Scrapling (FetcherClient directly)...")
    # FetcherClient is what Fetcher uses under the hood but Fetcher adds stealth
    client = FetcherClient()
    resp = client.get(sge_url, timeout=15)
    print(f"Status: {resp.status}")
    print(f"Content Length: {len(resp.text)}")
    print(f"Preview: {resp.text[:200]}")

if __name__ == "__main__":
    test_curl_cffi()
    test_scrapling_no_stealth()
