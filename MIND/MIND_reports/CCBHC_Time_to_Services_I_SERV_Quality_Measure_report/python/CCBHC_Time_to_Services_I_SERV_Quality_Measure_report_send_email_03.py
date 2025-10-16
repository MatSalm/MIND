import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.mime.image import MIMEImage
import os
from datetime import datetime
from configparser import ConfigParser
from dotenv import load_dotenv
import json
from pathlib import Path
from PIL import Image

# Load environment variables
load_dotenv()

# Paths
config_path = os.path.join('..', 'config', 'config.ini')
param_path = Path('temp_params.json')
data_path = Path('temp_data.pkl')
image_path1 = 'C:\\MIND\\MIND\\MIND_images\\HFS_Logo_FullColor_RGB_Large.png'
image_path2 = 'C:\\MIND\\MIND\\MIND_images\\MIND_logo.png'
resized_image1 = 'C:\\MIND\\MIND\\MIND_images\\HFS_Logo_FullColor_RGB_Large_resized.png'
resized_image2 = 'C:\\MIND\\MIND\\MIND_images\\MIND_logo_resized.png'

config = ConfigParser()
config.read(config_path)
to_email = config.get('email', 'to_email')

# Load parameters
with open(param_path, 'r') as f:
    params = json.load(f)
measure_year = params.get('measure_year', datetime.today().year)
run_date = datetime.today().strftime('%Y-%m-%d')

# File to attach
filename = f'CCBHC_Time_to_Services_I_SERV_Quality_Measure_report_{run_date}_MY{measure_year}.xlsx'
subject = f'I-SERV Quality Measure Report for MY{measure_year}'

# SMTP setup
smtp_email = os.getenv('EMAIL_smtp_email')
smtp_port = int(os.getenv('EMAIL_smtp_port', '587'))
smtp_server = os.getenv('EMAIL_smtp_server')

# Create email message
msg = MIMEMultipart('related')
msg['From'] = smtp_email
msg['To'] = to_email
msg['Subject'] = subject

# Email body
body = f"""
<html>
  <body style="font-family: 'Segoe UI', sans-serif; color: #242424;">
    <p>Hello,</p>
    <p style="margin-left: 40px;">Attached is the CCBHC I-SERV Quality Measure Report for measurement year {measure_year}.</p>
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

# Attach Excel report
part = MIMEBase('application', 'octet-stream')
with open(filename, 'rb') as file:
    part.set_payload(file.read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
msg.attach(part)

# Resize images
def resize_image(image_path, output_path, size):
    with Image.open(image_path) as img:
        img.thumbnail(size)
        img.save(output_path, format='PNG')

resize_image(image_path1, resized_image1, (200, 200))
resize_image(image_path2, resized_image2, (130, 130))

# Embed images
with open(resized_image1, 'rb') as img1:
    mime_img1 = MIMEImage(img1.read())
    mime_img1.add_header('Content-ID', '<image1>')
    msg.attach(mime_img1)

with open(resized_image2, 'rb') as img2:
    mime_img2 = MIMEImage(img2.read())
    mime_img2.add_header('Content-ID', '<image2>')
    msg.attach(mime_img2)

# Send the email
with smtplib.SMTP(smtp_server, smtp_port) as server:
    server.starttls()
    server.send_message(msg)

# Clean up temporary files
for temp_file in [filename, resized_image1, resized_image2, param_path, data_path]:
    try:
        os.remove(temp_file)
        print(f"Deleted: {temp_file}")
    except Exception as e:
        print(f"[WARN] Could not delete {temp_file}: {e}")

print("Email sent and all temporary files cleaned up.")
