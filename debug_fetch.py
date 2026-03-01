import json
from scrapling import Fetcher
from datetime import datetime, timedelta

fetcher = Fetcher()

def debug_shfe():
    # Try Friday
    date_str = "20260227"
    url = f"https://www.shfe.com.cn/data/dailydata/kx/pm{date_str}.dat"
    print(f"Testing SHFE: {url}")
    resp = fetcher.get(url)
    print(f"Status: {resp.status}")
    print(f"Content Length: {len(resp.text)}")
    print(f"First 200 chars: {resp.text[:200]}")
    try:
        data = json.loads(resp.text)
        print(f"JSON Keys: {data.keys()}")
        print(f"Record count: {len(data.get('o_cursor', []))}")
    except Exception as e:
        print(f"Not JSON: {e}")

def debug_sge():
    api_url = "https://www.sge.com.cn/public/front/findArticleExtList?pageNo=1&pageSize=15&menuId=1738"
    print(f"\nTesting SGE: {api_url}")
    resp = fetcher.get(api_url)
    print(f"Status: {resp.status}")
    print(f"Content Length: {len(resp.text)}")
    print(f"First 200 chars: {resp.text[:200]}")
    try:
        data = json.loads(resp.text)
        print(f"Record count: {len(data.get('list', []))}")
        if data.get('list'):
            print(f"First title: {data['list'][0].get('title')}")
            print(f"First fileUrl: {data['list'][0].get('fileUrl')}")
    except Exception as e:
        print(f"Not JSON: {e}")

if __name__ == "__main__":
    debug_shfe()
    debug_sge()
