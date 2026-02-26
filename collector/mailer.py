"""
邮件发送模块 - 使用 SMTP 发送 HTML 日报邮件。
"""
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from pathlib import Path
from typing import Any

from collector.settings import MAIL_CONFIG

logger = logging.getLogger(__name__)


class EmailSender:
    """SMTP 邮件发送器。"""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or MAIL_CONFIG

    def send_email(self, html_content: str, date: str, attachments: list[str | Path] | None = None) -> bool:
        """
        发送 HTML 格式的日报邮件，可选携带附件。

        Args:
            html_content: 完整 HTML 邮件正文。
            date: 报告日期（用于邮件标题）。
            attachments: 附件路径列表。

        Returns:
            发送成功返回 True，失败返回 False。
        """
        cfg = self.config
        subject = cfg["subject"].format(date=date)

        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = cfg["from_addr"]
        msg["To"] = ", ".join(cfg["to_addr"])

        # 邮件正文
        msg.attach(MIMEText(html_content, "html", "utf-8"))

        # 添加附件
        if attachments:
            for file_path in attachments:
                file_path = Path(file_path)
                if not file_path.exists():
                    logger.warning("附件文件不存在，跳过: %s", file_path)
                    continue
                
                try:
                    part = MIMEBase("application", "octet-stream")
                    with open(file_path, "rb") as f:
                        part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        f"attachment; filename={os.path.basename(file_path)}",
                    )
                    msg.attach(part)
                    logger.debug("已添加附件: %s", file_path)
                except Exception:
                    logger.exception("添加附件失败: %s", file_path)

        try:
            if cfg["smtp_port"] == 465:
                # SSL 连接
                server = smtplib.SMTP_SSL(cfg["smtp_server"], cfg["smtp_port"], timeout=30)
            else:
                # STARTTLS 连接
                server = smtplib.SMTP(cfg["smtp_server"], cfg["smtp_port"], timeout=30)
                server.starttls()

            server.login(cfg["username"], cfg["password"])
            server.sendmail(cfg["from_addr"], cfg["to_addr"], msg.as_string())
            server.quit()

            logger.info("📧 邮件发送成功: %s → %s (附件: %d)", subject, cfg["to_addr"], len(attachments) if attachments else 0)
            return True
        except smtplib.SMTPAuthenticationError:
            logger.error("邮件认证失败，请检查用户名和密码（163/QQ邮箱需使用授权码）")
            return False
        except Exception:
            logger.exception("邮件发送失败")
            return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sender = EmailSender()
    # 快速测试 - 发送一个简单的测试邮件
    test_html = "<h1>测试邮件</h1><p>这是贵金属日报系统的测试邮件。</p>"
    success = sender.send_email(test_html, "2026-01-01")
    print(f"{'✅ 发送成功' if success else '❌ 发送失败'}")
