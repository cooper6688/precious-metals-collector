# 贵金属日度自动化研究系统

> 每天自动抓取贵金属市场全域数据 → 计算关键因子 → 生成 HTML 报告 → 发邮件

## 系统架构

基于"四维多因子体系"搭建：

| 维度 | 数据源 | 频率 |
|------|--------|------|
| 库存与交割压力 | COMEX (akshare) / SHFE (官方API) | 日度 |
| 价格结构与基差 | CME (yfinance) / SHFE (akshare) | 日度 |
| 资金流与持仓 | GLD/SLV (yfinance) / CFTC (pycot-reports) | 日度/周度 |
| 实物供需 | 预留（年度手动更新） | 年度 |

## 快速开始

### 1. 创建虚拟环境

```bash
cd project_root
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/macOS
source .venv/bin/activate
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置邮箱（可选）

设置环境变量或编辑 `collector/settings.py`:

```bash
# Windows PowerShell
$env:PM_SMTP_SERVER = "smtp.qq.com"
$env:PM_SMTP_PORT = "587"
$env:PM_SMTP_USER = "your_email@qq.com"
$env:PM_SMTP_PASS = "your_app_password"
$env:PM_SMTP_FROM = "your_email@qq.com"
$env:PM_SMTP_TO = "target@example.com"
```

### 4. 运行

```bash
# 测试运行（不发邮件）
python collector/run_daily.py --dry-run

# 正式运行
python collector/run_daily.py

# 指定日期
python collector/run_daily.py --date 2026-02-01 --dry-run
```

### 5. 定时任务

**Windows 任务计划程序:**

```powershell
schtasks /create /tn "PreciousMetalReport" /tr "D:\path\to\.venv\Scripts\python.exe D:\path\to\collector\run_daily.py" /sc daily /st 18:00
```

**Linux crontab:**

```bash
0 18 * * * /path/to/.venv/bin/python /path/to/collector/run_daily.py >> /path/to/logs/cron.log 2>&1
```

## 项目结构

```
project_root/
  collector/
    settings.py           # 全局配置
    database.py           # SQLite 数据库管理
    mailer.py             # 邮件发送
    run_daily.py          # 主入口
    data_fetcher/         # 数据抓取
      inventory_fetcher.py
      price_fetcher.py
      etf_fetcher.py
      cftc_fetcher.py
    calculator/           # 指标计算
      inventory_calculator.py
      price_calculator.py
      funding_calculator.py
    reporter/             # 报告生成
      html_template.html
      report_generator.py
  data/                   # SQLite 数据库 & 报告
  logs/                   # 运行日志
```

## 核心指标

- **DPI** (交割压力指数) = 注册库存 / OI
- **基差** = 近月期货 - 现货
- **期限结构斜率** → Contango / Backwardation
- **ETF 净流入** = 当日份额 - 前日份额
- **投机拥挤度** = CFTC 净头寸 / 注册库存
- **综合景气度** (0-100分) 多因子加权评分
