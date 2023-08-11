from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib

class EmailSender:
    HOST = "smtp.gmail.com"
    PORT = "587"
    EMAIL_ADDRESS = "wmcubeintern@gmail.com"
    EMAIL_PASSWORD = "swynlmbrjjqetmpm"
    
    recipients = []
    
    def __init__(self, *recipients: str):
        self.recipients = recipients
    
    def send(self, filename: str):
        content = MIMEMultipart()        

        content["from"] = self.EMAIL_ADDRESS
        content['To'] = ", ".join(self.recipients)

        with open(filename, "rb") as fs:
            part = MIMEBase('application', "octet-stream")
            part.set_payload(fs.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        content.attach(part)
        
        content["subject"] = filename
        content.attach(MIMEText(f"The NAV feed for the latest one month period has been attached."))

        with smtplib.SMTP(host=self.HOST, port=self.PORT) as smtp:
            try:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(self.EMAIL_ADDRESS, self.EMAIL_PASSWORD)
                smtp.send_message(content)
            except Exception as e:
                print("Error message:", e)
