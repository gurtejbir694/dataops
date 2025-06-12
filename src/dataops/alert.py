import smtplib
from email.mime.text import MIMEText
from dataops.logging import setup_logger
from dotenv import load_dotenv
import os

def send_alert(subject: str, body: str, config: dict, verbose: bool):
    logger = setup_logger(verbose, Path(config["log_dir"]))
    load_dotenv()
    smtp_user = os.getenv("SMTP_USER", "your_email@example.com")
    smtp_password = os.getenv("SMTP_PASSWORD", "your_app_password")
    
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = "recipient@example.com"
    
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        logger.info("Alert sent successfully")
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")
        raise
