import requests
import urllib3
import traceback
from bs4 import BeautifulSoup

urllib3.disable_warnings()

print("--- Testing SHFE (2026-02-27.dat) ---")
try:
    resp = requests.get('https://www.shfe.com.cn/data/dailydata/kx/pm20260227.dat', verify=False, timeout=10)
    print(f"SHFE 0227 HTTP Status: {resp.status_code}")
except Exception as e:
    print(f"SHFE 0227 Error: {e}")

print("\n--- Testing SHFE (2026-02-26.dat) ---")
try:
    resp = requests.get('https://www.shfe.com.cn/data/dailydata/kx/pm20260226.dat', verify=False, timeout=10)
    print(f"SHFE 0226 HTTP Status: {resp.status_code}")
except Exception as e:
    print(f"SHFE 0226 Error: {e}")

print("\n--- Testing SGE HTML ---")
try:
    resp = requests.get('https://www.sge.com.cn/sjzx/mrhq', verify=False, timeout=10)
    soup = BeautifulSoup(resp.text, 'html.parser')
    links = [a.get_text().strip() for a in soup.find_all('a')]
    print(f"Total links: {len(links)}")
    
    keywords = ["交割", "数据", "报告", "仓单"]
    for i, a in enumerate(soup.find_all('a')):
        text = a.get_text().strip()
        if any(k in text for k in keywords):
            print(f"Found keyword in link: '{text}' -> href={a.get('href', '')}")
except Exception as e:
    print("SGE error:")
    traceback.print_exc()

print("\n--- Testing LBMA Yahoo API ---")
try:
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/XAUUSD=X?range=5d&interval=1d", headers=headers, timeout=10)
    print(f"Yahoo HTTP Status: {resp.status_code}")
    print(resp.text[:200])
except Exception as e:
    print(f"Yahoo error: {e}")

