
import os
import sys
from scrapling import StealthyFetcher

def debug_sge_table():
    fetcher = StealthyFetcher()
    url = "https://www.sge.com.cn/sjzx/quotation_daily_new"
    
    # 模拟点击查询按钮
    page_action = """
    async ({page, response}) => {
        await page.click('input[type="button"][value="查询"]');
        await page.waitForSelector('.table-list tr', {timeout: 10000});
    }
    """
    
    resp = fetcher.fetch(url, page_action=page_action, wait=2000)
    rows = resp.css(".table-list tr")
    
    print(f"Total rows found: {len(rows)}")
    for i, row in enumerate(rows[:10]):
        cells = [c.text.strip() for c in row.css("td")]
        if cells:
             print(f"Row {i}: {cells}")
        else:
             print(f"Row {i} (Header?): {[c.text.strip() for c in row.css('th')]}")

if __name__ == "__main__":
    debug_sge_table()
