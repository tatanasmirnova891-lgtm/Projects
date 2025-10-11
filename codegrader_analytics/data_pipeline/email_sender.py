import smtplib
import ssl
from email.message import EmailMessage
import logging


class EmailSender:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def send(self):
        try:
            msg = EmailMessage()
            msg.set_content(self.config['email_body'])
            msg['Subject'] = self.config['email_subject']
            msg['From'] = self.config['sender_email']
            msg['To'] = self.config['receiver_email']

            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.config['smtp_server'], self.config['smtp_port'], context=context) as server:
                server.login(self.config['sender_email'], self.config['email_password'])
                server.send_message(msg)
            self.logger.info("Email отправлен")
        except Exception as e:
            self.logger.error(f"Ошибка при отправке email: {e}")
            raise
