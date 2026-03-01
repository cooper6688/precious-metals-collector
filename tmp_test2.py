import curl_cffi.requests as requests
import json
import traceback

print("=== SGE JSON API ===")
try:
    url = "https://www.sge.com.cn/public/front/findArticleExtList?pageNo=1&pageSize=15&menuId=1738"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://www.sge.com.cn/sjzx/mrhq"
    }
    resp = requests.get(url, headers=headers, impersonate="chrome110", timeout=10)
    print("SGE Status:", resp.status_code)
    data = resp.json()
    items = data.get("list", [])
    for item in items[:5]:
        print(f"[{item.get('publishDate', '')}] {item.get('title', '')} -> {item.get('fileUrl', '')} / {item.get('id', '')}")
except Exception as e:
    print("SGE JSON Error:", e)

print("\n=== SHFE URL Checking ===")
try:
    url2 = "https://www.shfe.com.cn/data/dailydata/kx/pm20260225.dat"
    resp2 = requests.get(url2, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.shfe.com.cn/"}, impersonate="chrome110", timeout=10)
    print("SHFE 20260225 Status:", resp2.status_code)
    
    url3 = "http://www.shfe.com.cn/data/dailydata/kx/pm20260225.dat"
    resp3 = requests.get(url3, headers={"User-Agent": "Mozilla/5.0"}, impersonate="chrome110", timeout=10)
    print("SHFE HTTP 20260225 Status:", resp3.status_code)
except Exception as e:
    print("SHFE Error:", e)

print("\n=== AKShare LBMA Alternatives ===")
try:
    import akshare as ak
    df = ak.futures_global_spot_url() if hasattr(ak, 'futures_global_spot_url') else None
    print("Has futures_global_spot_url?", df is not None)
    
    print("Fetching ak.spot_goods_sina...")
    try:
        print(ak.spot_goods_sina(symbol="XAU"))
    except Exception as e:
        print("spot_goods_sina error:", e)
        
    print("Fetching ak.futures_zh_spot...")
    try:
        # maybe another symbol
        pass
    except Exception as e:
        pass
except Exception as e:
    print("AKShare check error:", e)
