import re
import io
import requests
from bs4 import BeautifulSoup
import pdfplumber

def test_sge_pdf():
    url = "https://www.sge.com.cn/sjzx/mrhq"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    print("Scraping SGE...")
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # 寻找包含“延期交割情况”或类似名称的链接
        links = soup.find_all('a')
        target_href = None
        for a in links:
            text = a.get_text(strip=True)
            if "延期交割情况" in text or "数据概览" in text or "交割" in text:
                href = a.get("href")
                if href and href.endswith(".pdf"):
                    target_href = href
                    print(f"Found PDF link: {text} -> {href}")
                    break
                elif href and "article" in href: # Sometimes it is nested in an article page
                    target_href = href
                    print(f"Found Article link: {text} -> {href}")
                    break
                    
        if not target_href:
            print("No delivery PDF found on the main page.")
            return

        if not target_href.startswith("http"):
            target_href = "https://www.sge.com.cn" + target_href
            
        # 如果是文章页面，需要再抓取一次里面的 PDF
        if not target_href.endswith(".pdf"):
            resp2 = requests.get(target_href, headers=headers, timeout=15)
            soup2 = BeautifulSoup(resp2.text, 'html.parser')
            pdf_a = soup2.find('a', href=re.compile(r'\.pdf$'))
            if pdf_a:
                target_href = "https://www.sge.com.cn" + pdf_a.get("href")
            else:
                print("No PDF found inside article.")
                return

        print(f"Downloading PDF: {target_href}")
        pdf_resp = requests.get(target_href, headers=headers, timeout=15)
        with pdfplumber.open(io.BytesIO(pdf_resp.content)) as pdf:
            print(f"PDF Pages: {len(pdf.pages)}")
            if len(pdf.pages) > 0:
                page = pdf.pages[0]
                text = page.extract_text()
                print("--- Text Snippet ---")
                print(text[:500] if text else "No text")
                tables = page.extract_tables()
                if tables:
                    print("--- Table Snippet ---")
                    for row in tables[0][:5]:
                        print(row)
                else:
                    print("No tables extracted.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_sge_pdf()
