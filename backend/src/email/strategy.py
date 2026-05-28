import os, smtplib, logging
from abc import ABC, abstractmethod
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class EmailStrategy(ABC):

    @abstractmethod
    def send(
        self,
        to: str,
        subject: str,
        body: str
    ) -> bool:
        pass


class SMTPStrategy(EmailStrategy):

    def __init__(self):
        self.host = os.getenv('EMAIL_HOST', 'smtp.gmail.com')
        self.port = int(os.getenv('EMAIL_PORT', '465'))
        self.user = os.getenv('EMAIL_ADDRESS')
        self.password = os.getenv('EMAIL_PASSWORD')

        if not self.user or not self.password:
            raise ValueError(
                'EMAIL_ADDRESS and EMAIL_PASSWORD must be set in .env. '
                'Use a Gmail App Password, not your normal password.'
            )

    def send(
        self,
        to: str,
        subject: str,
        body: str
    ) -> bool:
        msg = MIMEMultipart('alternative')

        msg['Subject'] = subject
        msg['From'] = self.user
        msg['To'] = to

        msg.attach(MIMEText(body, 'html'))

        with smtplib.SMTP_SSL(
            self.host,
            self.port
        ) as server:
            server.login(
                self.user,
                self.password
            )

            server.sendmail(
                self.user,
                to,
                msg.as_string()
            )

        logger.info(
            f'Email sent via SMTP to {to}: {subject}'
        )

        return True


class SendGridStrategy(EmailStrategy):

    def __init__(self):
        self.api_key = os.getenv('SENDGRID_API_KEY')
        self.from_email = os.getenv('EMAIL_ADDRESS')

        if not self.api_key:
            raise ValueError(
                'SENDGRID_API_KEY must be set in .env'
            )

    def send(
        self,
        to: str,
        subject: str,
        body: str
    ) -> bool:
        raise NotImplementedError(
            'SendGrid not yet configured. See comments above.'
        )


def email_strategy_factory() -> EmailStrategy:
    provider = os.getenv(
        'EMAIL_PROVIDER',
        'smtp'
    ).lower().strip()

    if provider == 'sendgrid':
        logger.info(
            'Using SendGrid email strategy'
        )
        return SendGridStrategy()

    logger.info(
        'Using SMTP email strategy'
    )

    return SMTPStrategy()