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
from datetime import datetime
import configparser
from dotenv import load_dotenv
import json
import sys

# Function to resize images in memory
def resize_image(image_path, size):
    with Image.open(image_path) as img:
        img.thumbnail(size)
        img_byte_array = BytesIO()
        img.save(img_byte_array, format='PNG')
        img_byte_array.seek(0)
        return img_byte_array

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read('../config/config.ini')
to_email = config['email']['to_email']

# Load environment variables from .env
load_dotenv()

EMAIL_smtp_email = os.getenv('EMAIL_smtp_email')
EMAIL_smtp_port = os.getenv('EMAIL_smtp_port')
EMAIL_smtp_server = os.getenv('EMAIL_smtp_server')

# Load date parameters from temp_params.json
params_file_path = 'temp_params.json'  # Adjust the path as necessary
try:
    with open(params_file_path, 'r') as file:
        params = json.load(file)
    start_date = datetime.strptime(params['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(params['end_date'], '%Y-%m-%d')
except Exception as e:
    print(f"Error reading date parameters from JSON: {e}")
    sys.exit(1)

# Find the latest Excel file in the current working directory
current_directory = Path('.')
matching_files = list(current_directory.glob(f'columbia_assessment_sample_{start_date.strftime("%Y-%m-%d")}_to_{end_date.strftime("%Y-%m-%d")}.xlsx'))
if not matching_files:
    print("No Excel files found to attach. Email not sent.")
    sys.exit(1)
latest_file = max(matching_files, key=os.path.getctime)

# Email setup
smtp_email = EMAIL_smtp_email
smtp_port = int(EMAIL_smtp_port)
smtp_server = EMAIL_smtp_server

# Set up the SMTP server
server = smtplib.SMTP(smtp_server, smtp_port)
server.ehlo()  # Identify yourself to the SMTP server
server.starttls()
server.ehlo()  # Identify yourself to the SMTP server again after starting TLS

# Create the email
msg = MIMEMultipart('related')  # 'related' is used to send images embedded in the email
msg['From'] = smtp_email
msg['To'] = to_email
msg['Subject'] = f'Columbia Assessment Sample Report {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}'

# Create the body of the email
body = f"""
<html>
  <body style="font-family: 'Segoe UI', sans-serif; color: #242424;">
    <p>Hello,</p>
    <p style="margin-left: 40px;">Attached is the Columbia Assessment Sample Report for {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}.</p>
    <p>Thank you,</p>
    <div style="margin-top: 20px;">
      <table cellspacing="0" cellpadding="0" border="0" style="width: auto;">
        <tr>
          <td colspan="2">
            <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 10px 0;">
          </td>
        </tr>
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

# Attach the Excel file
attachment_name = latest_file.name
part = MIMEBase('application', 'octet-stream')
with open(latest_file, 'rb') as attachment:
    part.set_payload(attachment.read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', f'attachment; filename="{attachment_name}"')
msg.attach(part)

# Resize and embed the signature image in the email
signature_image_path = 'C:\\MIND\\MIND\\MIND_images\\HFS_Logo_FullColor_RGB_Large.png'
resized_signature_image = resize_image(signature_image_path, (200, 200))
msg_image = MIMEImage(resized_signature_image.read())
msg_image.add_header('Content-ID', '<image1>')
msg.attach(msg_image)

# Resize and embed the additional image in the email
additional_image_path = 'C:\\MIND\\MIND\\MIND_images\\MIND_logo.png'
resized_additional_image = resize_image(additional_image_path, (130, 130))
msg_image2 = MIMEImage(resized_additional_image.read())
msg_image2.add_header('Content-ID', '<image2>')
msg.attach(msg_image2)

# Send the email
server.send_message(msg)
server.quit()

print("Email sent successfully.")

# Cleanup: remove the .pkl, .json, and .xlsx files
for file_extension in ['.pkl', '.json', '.xlsx']:
    for file in current_directory.glob(f'*{file_extension}'):
        os.remove(file)
print("Temporary files removed successfully.")
