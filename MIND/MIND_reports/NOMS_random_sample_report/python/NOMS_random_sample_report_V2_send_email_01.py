import smtplib
import os
import time
from datetime import datetime
from pathlib import Path
from PIL import Image
from configparser import ConfigParser
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.mime.image import MIMEImage

# Load environment variables
load_dotenv(dotenv_path='C:/MIND/MIND/MIND_config/MIND.env')

# Define the path to the config.ini file
config_file_path = os.path.join('C:\\MIND\\MIND_reports\\NOMS_random_sample_report', 'config', 'config.ini')

# Load configuration
config = ConfigParser()
config.read(config_file_path)

# Extract email recipient
to_email = config.get('email', 'to_email')

# Extract email configuration from environment variables
smtp_email = os.getenv('EMAIL_smtp_email')
smtp_port = int(os.getenv('EMAIL_smtp_port'))
smtp_server = os.getenv('EMAIL_smtp_server')

# Validate email settings
if not smtp_email or not smtp_port or not smtp_server:
    raise ValueError("SMTP configuration is missing in the environment variables.")

# Define report details
report_date = datetime.now().strftime('%Y_%m_%d')
attachment_name = f"NOMS_Sample_Report_{report_date}.xlsx"
attachment_path = os.path.join('C:\\MIND\\MIND_reports\\NOMS_random_sample_report', 'logs', attachment_name)

# Ensure attachment exists
if not os.path.exists(attachment_path):
    raise FileNotFoundError(f"Attachment not found: {attachment_path}")

# Email subject
subject = f'NOMs Random Sample Report for {report_date}'

# Email content (HTML format)
body = f"""
<html>
  <body style="font-family: 'Segoe UI', sans-serif; color: #242424;">
    <p>Hello,</p>
    <p style="margin-left: 40px;">Attached is the NOMs random sample report for {report_date}.</p>
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

# Initialize the email message
msg = MIMEMultipart('related')
msg['From'] = smtp_email
msg['To'] = to_email
msg['Subject'] = subject
msg.attach(MIMEText(body, 'html'))

# Function to resize images
def resize_image(image_path, output_path, size):
    with Image.open(image_path) as img:
        img.thumbnail(size)
        img.save(output_path, format='PNG')

# Image paths
signature_image_path = 'C:\\MIND\\MIND\\MIND_images\\HFS_Logo_FullColor_RGB_Large.png'
resized_signature_image_path = 'C:\\MIND\\MIND\\MIND_images\\HFS_Logo_FullColor_RGB_Large_resized.png'

additional_image_path = 'C:\\MIND\\MIND\\MIND_images\\MIND_logo.png'
resized_additional_image_path = 'C:\\MIND\\MIND\\MIND_images\\MIND_logo_resized.png'

# Resize and embed the signature image
resize_image(signature_image_path, resized_signature_image_path, (200, 200))
with open(resized_signature_image_path, 'rb') as img:
    msg_image = MIMEImage(img.read())
    msg_image.add_header('Content-ID', '<image1>')
    msg.attach(msg_image)

# Resize and embed the additional image
resize_image(additional_image_path, resized_additional_image_path, (130, 130))
with open(resized_additional_image_path, 'rb') as img:
    msg_image2 = MIMEImage(img.read())
    msg_image2.add_header('Content-ID', '<image2>')
    msg.attach(msg_image2)

# Attach the Excel file
part = MIMEBase('application', 'octet-stream')
with open(attachment_path, 'rb') as attachment:
    part.set_payload(attachment.read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', f'attachment; filename="{attachment_name}"')
msg.attach(part)

# Send email with retry logic
for attempt in range(4):
    try:
        server = smtplib.SMTP(smtp_server, smtp_port, timeout=60)
        server.starttls()
        server.send_message(msg)
        server.quit()
        print(f"NOMs sample report for {report_date} emailed successfully.")
        break
    except Exception as e:
        print(f"Email attempt {attempt + 1} failed: {e}")
        if attempt < 3:
            time.sleep(5)  # Wait before retrying
        else:
            raise

# Cleanup: Delete temporary files
temp_files = [
    'C:\\MIND\\MIND_reports\\NOMS_random_sample_report\\python\\temp_data.pkl',
    'C:\\MIND\\MIND_reports\\NOMS_random_sample_report\\python\\temp_params.json',
    resized_signature_image_path,
    resized_additional_image_path
]

for file_path in temp_files:
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted: {file_path}")

print("All temporary files cleaned up successfully.")
