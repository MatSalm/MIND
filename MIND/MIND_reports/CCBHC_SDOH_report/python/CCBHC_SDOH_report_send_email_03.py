#!/usr/bin/env python3
# 03.py - Email the CCBHC SDOH report
# Follows the same pattern as the I‑SERV report mailer but adapted for the
# file naming conventions used in 02.py.

import os
import smtplib
import json
from datetime import datetime
from pathlib import Path
from configparser import ConfigParser
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from dotenv import load_dotenv
from PIL import Image

# ----------------------------------------------------------------------
# Paths & constants
# ----------------------------------------------------------------------
CONFIG_PATH      = os.path.join('..', 'config', 'config.ini')
PARAM_PATH       = Path('temp_params.json')
DATA_PATH        = Path('temp_data.pkl')

IMG_HFS_ORIG     = 'C:\\MIND\\MIND\\MIND_images\\HFS_Logo_FullColor_RGB_Large.png'
IMG_MIND_ORIG    = 'C:\\MIND\\MIND\\MIND_images\\MIND_logo.png'
IMG_HFS_SMALL    = 'C:\\MIND\\MIND\\MIND_images\\HFS_Logo_FullColor_RGB_Large_resized.png'
IMG_MIND_SMALL   = 'C:\\MIND\\MIND\\MIND_images\\MIND_logo_resized.png'

# ----------------------------------------------------------------------
# Load configuration and env vars
# ----------------------------------------------------------------------
load_dotenv()
config = ConfigParser()
config.read(CONFIG_PATH)
TO_EMAIL = config.get('email', 'to_email')

SMTP_EMAIL  = os.getenv('EMAIL_smtp_email')
SMTP_PORT   = int(os.getenv('EMAIL_smtp_port', '587'))
SMTP_SERVER = os.getenv('EMAIL_smtp_server')

# ----------------------------------------------------------------------
# Parameters & file names
# ----------------------------------------------------------------------
with open(PARAM_PATH, 'r') as f:
    params = json.load(f)

MEASURE_YEAR = params.get('measure_year') or str(datetime.today().year)
RUN_DT       = datetime.today()
RUN_LABEL    = RUN_DT.strftime('%Y-%m-%d')
RUN_STAMP    = RUN_DT.strftime('%Y%m%d')

REPORT_NAME  = f'SDOH_Summary_Report_MY{MEASURE_YEAR}_{RUN_STAMP}.xlsx'
SUBJECT      = f'SDOH Screening Summary Report for MY{MEASURE_YEAR}'

# ----------------------------------------------------------------------
# Resize helper (idempotent)
# ----------------------------------------------------------------------

def resize_image(src: str, dest: str, size: tuple[int, int]):
    if os.path.exists(dest):
        return  # already resized
    with Image.open(src) as img:
        img.thumbnail(size)
        img.save(dest, format='PNG')

resize_image(IMG_HFS_ORIG,  IMG_HFS_SMALL,  (200, 200))
resize_image(IMG_MIND_ORIG, IMG_MIND_SMALL, (130, 130))

# ----------------------------------------------------------------------
# Build the email
# ----------------------------------------------------------------------
msg = MIMEMultipart('related')
msg['From']    = SMTP_EMAIL
msg['To']      = TO_EMAIL
msg['Subject'] = SUBJECT

HTML_BODY = f"""
<html>
  <body style='font-family: Segoe UI, sans-serif; color: #242424;'>
    <p>Hello,</p>
    <p style='margin-left: 40px;'>Attached is the CCBHC SDOH Screening Summary Report for measurement year {MEASURE_YEAR}.</p>
    <p>Thank you,</p>
    <div style='margin-top: 20px;'>
      <table cellspacing='0' cellpadding='0' border='0'>
        <tr><td colspan='2'><hr style='border: none; border-top: 1px solid #e0e0e0; margin: 10px 0;'></td></tr>
        <tr>
          <td style='vertical-align: top; padding-right: 10px;'>
            <p style='margin: 0; font-size: 14px;'><strong>MIND</strong><br>
            Enterprise Reporting System,<br>
            Hillcrest Family Services</p>
            <p style='margin: 5px 0;'>
              <a href='https://hillcrest-fs.org/' style='color: #467886; text-decoration: none;'>hillcrest-fs.org</a>
            </p>
          </td>
          <td style='border-left: 1px solid #e0e0e0; padding-left: 10px; vertical-align: top;'>
            <img src='cid:image_mind' style='height: 50px; margin-top: 10px;'>
            <img src='cid:image_hfs'  style='height: 50px; margin-left: 10px; margin-top: 10px;'>
          </td>
        </tr>
      </table>
    </div>
  </body>
</html>
"""
msg.attach(MIMEText(HTML_BODY, 'html'))

# ----------------------------------------------------------------------
# Attach the Excel report
# ----------------------------------------------------------------------
part = MIMEBase('application', 'octet-stream')
with open(REPORT_NAME, 'rb') as fp:
    part.set_payload(fp.read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', f'attachment; filename="{REPORT_NAME}"')
msg.attach(part)

# ----------------------------------------------------------------------
# Embed images
# ----------------------------------------------------------------------
for cid, path in (('image_hfs', IMG_HFS_SMALL), ('image_mind', IMG_MIND_SMALL)):
    with open(path, 'rb') as f:
        mime_img = MIMEImage(f.read())
        mime_img.add_header('Content-ID', f'<{cid}>')
        msg.attach(mime_img)

# ----------------------------------------------------------------------
# Send the email
# ----------------------------------------------------------------------
with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
    server.starttls()
    server.send_message(msg)

# ----------------------------------------------------------------------
# Clean‑up
# ----------------------------------------------------------------------
for file_path in [REPORT_NAME, IMG_HFS_SMALL, IMG_MIND_SMALL, PARAM_PATH, DATA_PATH]:
    try:
        os.remove(file_path)
        print(f'Deleted: {file_path}')
    except Exception as exc:
        print(f'[WARN] Could not delete {file_path}: {exc}')

print('Email sent and temporary files cleaned up.')
