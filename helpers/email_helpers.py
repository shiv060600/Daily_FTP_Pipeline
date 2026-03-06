import logging
import smtplib
from email.message import EmailMessage

from helpers.ENV import EMAIL_CONFIG


def send_failure_email(error_message: str) -> None:
    """Send email notification when daily file processing fails."""
    try:
        msg = EmailMessage()
        msg["Subject"] = "Daily File FAILED - Action Required"
        msg["From"] = EMAIL_CONFIG["EMAIL_USER"]
        msg["To"] = "sbhutani@tuttlepublishing.com"
        msg.set_content(
            f"Daily file has FAILED please check\n\nError Details:\n{error_message}"
        )

        with smtplib.SMTP(
            EMAIL_CONFIG["SMTP_SERVER"], EMAIL_CONFIG["SMTP_PORT"]
        ) as server:
            server.starttls()
            server.login(
                EMAIL_CONFIG["EMAIL_USER"], EMAIL_CONFIG["EMAIL_PASSWORD"]
            )
            server.send_message(msg)
            logging.info(
                "Failure notification email sent to sbhutani@tuttlepublishing.com"
            )
    except Exception as e:
        logging.error(f"Failed to send failure notification email: {e}")

