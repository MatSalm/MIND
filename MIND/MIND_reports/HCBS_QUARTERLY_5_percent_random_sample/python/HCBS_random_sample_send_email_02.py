import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.mime.image import MIMEImage
import os
from pathlib import Path
from io import BytesIO
from PIL import Image
import configparser
from dotenv import load_dotenv

# Resize image in memory
def resize_image(image_path, size):
    with Image.open(image_path) as img:
        img.thumbnail(size)
        img_byte_array = BytesIO()
        img.save(img_byte_array, format='PNG')
        img_byte_array.seek(0)
        return img_byte_array

# Load config
config = configparser.ConfigParser()
config.read('../config/config.ini')
to_email = config['email']['to_email']

load_dotenv('../.env')
smtp_email = os.getenv('EMAIL_smtp_email')
smtp_port = int(os.getenv('EMAIL_smtp_port'))
smtp_server = os.getenv('EMAIL_smtp_server')

# Locate latest output file
current_directory = Path('.')
latest_file = max(current_directory.glob('All_Program_Regulatory_Audit_random_sample_*.xlsx'), key=os.path.getctime)
date_tag = latest_file.stem.replace('All_Program_Regulatory_Audit_random_sample_', '')

# Build email
msg = MIMEMultipart('related')
msg['From'] = smtp_email
msg['To'] = to_email
msg['Subject'] = f'All Program Regulatory Audit Random Sample Report – {date_tag}'

body = f"""
<html>
  <body style="font-family: 'Segoe UI', sans-serif; color: #242424;">
    <p>Hello,</p>
    <p style="margin-left: 40px;">Attached is the random sample report for the All Program Regulatory Audit dated {date_tag}.</p>
    <p>Thank you,</p>
    <div style="margin-top: 20px;">
      <table cellspacing="0" cellpadding="0" border="0" style="width: auto;">
        <tr><td colspan="2"><hr style="border: none; border-top: 1px solid #e0e0e0; margin: 10px 0;"></td></tr>
        <tr>
          <td style="vertical-align: top; padding-right: 10px;">
            <p style="margin: 0; font-size: 14px;"><strong>MIND</strong><br>
            Enterprise Reporting System,<br>
            Hillcrest Family Services</p>
            <p style="margin: 5px 0;">
              <a href="https://hillcrest-fs.org/" style="color: #467886; text-decoration: none;">hillcrest-fs.org</a>
            </p>
          </td>
          <td style="border-left: 1px solid #e0e0e0; padding-left: 10px; vertical-align: top;">
            <img src="cid:image2" style="height: 50px; margin-top: 10px;">
            <img src="cid:image1" style="height: 50px; margin-left: 10px; margin-top: 10px;">
          </td>
        </tr>
      </table>
    </div>
  </body>
</html>
"""
msg.attach(MIMEText(body, 'html'))

# Attach Excel file
part = MIMEBase('application', 'octet-stream')
with open(latest_file, 'rb') as attachment:
    part.set_payload(attachment.read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', f'attachment; filename="{latest_file.name}"')
msg.attach(part)

# Attach images
sig_path = 'C:\\MIND\\MIND\\MIND_images\\HFS_Logo_FullColor_RGB_Large.png'
logo_path = 'C:\\MIND\\MIND\\MIND_images\\MIND_logo.png'

sig_image = resize_image(sig_path, (200, 200))
msg_image1 = MIMEImage(sig_image.read())
msg_image1.add_header('Content-ID', '<image1>')
msg.attach(msg_image1)

logo_image = resize_image(logo_path, (130, 130))
msg_image2 = MIMEImage(logo_image.read())
msg_image2.add_header('Content-ID', '<image2>')
msg.attach(msg_image2)

# Send the email
with smtplib.SMTP(smtp_server, smtp_port) as server:
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.send_message(msg)

print("Email sent successfully.")

# Clean up
for ext in ['.pkl', '.json', '.xlsx']:
    for file in current_directory.glob(f'*{ext}'):
        os.remove(file)

print("Temporary files removed successfully.")
