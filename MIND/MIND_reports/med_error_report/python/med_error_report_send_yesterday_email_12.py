import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.mime.image import MIMEImage
import os
from datetime import datetime
import json
from pathlib import Path
from PIL import Image
from configparser import ConfigParser
from dotenv import load_dotenv
import pandas as pd
from io import BytesIO

# Load environment variables from the .env file
load_dotenv()

# Function to resize images in memory
def resize_image_in_memory(image_path, size):
    with Image.open(image_path) as img:
        img.thumbnail(size)
        img_byte_array = BytesIO()
        img.save(img_byte_array, format='PNG')
        img_byte_array.seek(0)
        return img_byte_array

# Define the path to the config.ini file
config_file_path = os.path.join('..', 'config', 'config.ini')

# Load the parameters from the config.ini file
config = ConfigParser()
config.read(config_file_path)

# Extract parameters from the loaded config
to_email = config.get('email', 'to_email')

# Load the parameters from the temp_params.json file
temp_param_file = Path('temp_params.json')
temp_data_file = Path('temp_data.pkl')  # Ensure this is defined
with open(temp_param_file, 'r') as f:
    parameters = json.load(f)

# Extract filter_date from the loaded JSON
filter_date = parameters['filter_date']

# Extract email configuration from environment variables
smtp_email = os.getenv('EMAIL_smtp_email')
smtp_port = os.getenv('EMAIL_smtp_port')
smtp_server = os.getenv('EMAIL_smtp_server')

# Check if environment variables are loaded correctly
if not smtp_email or not smtp_port or not smtp_server:
    raise ValueError("SMTP configuration is missing in the environment variables.")

# Convert smtp_port to integer
smtp_port = int(smtp_port)

# Format the filter_date for the subject and filename
filter_date_formatted = pd.to_datetime(filter_date).strftime('%Y-%m-%d')
subject = f'No Documentation Med Error Report for {filter_date_formatted}'
body_date_info = f'for {filter_date_formatted}'

# Set up the SMTP server
server = smtplib.SMTP(smtp_server, smtp_port)
server.ehlo()  # Identify yourself to the SMTP server
server.starttls()
server.ehlo()  # Identify yourself to the SMTP server again after starting TLS

# Create the email
msg = MIMEMultipart('related')  # 'related' is used to send images embedded in the email
msg['From'] = smtp_email
msg['To'] = to_email
msg['Subject'] = subject

# Create the body of the email
body = f"""
<html>
  <body style="font-family: 'Segoe UI', sans-serif; color: #242424;">
    <p>Hello,</p>
    <p style="margin-left: 40px;">Attached is the No Documentation Med Error Report {body_date_info}.</p>
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
attachment_name = f'no_documentation_med_error_report_for_{filter_date_formatted}.xlsx'
part = MIMEBase('application', 'octet-stream')
with open(attachment_name, 'rb') as attachment:
    part.set_payload(attachment.read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', f'attachment; filename="{attachment_name}"')
msg.attach(part)

# Resize and embed the signature image in the email
signature_image_path = 'C:\\MIND\\MIND\\MIND_images\\HFS_Logo_FullColor_RGB_Large.png'
resized_signature_image = resize_image_in_memory(signature_image_path, (200, 200))
msg_image = MIMEImage(resized_signature_image.read())
msg_image.add_header('Content-ID', '<image1>')
msg.attach(msg_image)

# Resize and embed the additional image in the email
additional_image_path = 'C:\\MIND\\MIND\\MIND_images\\MIND_logo.png'
resized_additional_image = resize_image_in_memory(additional_image_path, (130, 130))
msg_image2 = MIMEImage(resized_additional_image.read())
msg_image2.add_header('Content-ID', '<image2>')
msg.attach(msg_image2)

# Send the email
server.send_message(msg)
server.quit()

# Delete the files
os.remove(attachment_name)  # Delete the Excel file

# Delete the temporary files
os.remove(temp_param_file)  # Delete the temp_params.json file
os.remove(temp_data_file)  # Delete the temp_data.pkl file

print("Files deleted successfully.")
