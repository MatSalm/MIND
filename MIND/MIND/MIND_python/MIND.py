import os
import subprocess
from pathlib import Path
import sys
import pickle
import json
import datetime
from dotenv import load_dotenv
import logging
import re
import configparser
import smtplib
from email.mime.text import MIMEText

# Load environment variables from MIND.env file
load_dotenv(dotenv_path='C:/MIND/MIND/MIND_config/MIND.env')

def setup_logging(log_file_path):
    logging.basicConfig(
        filename=log_file_path,
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s:%(message)s'
    )

def send_email(subject, message, recipient):
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = os.getenv('EMAIL_smtp_email')
    msg['To'] = recipient
    server = smtplib.SMTP(os.getenv('EMAIL_smtp_server'), int(os.getenv('EMAIL_smtp_port')))
    server.starttls()
    server.send_message(msg)
    server.quit()

def run_script(script_path, data, parameters, log_file, first_script=False):
    try:
        cwd = script_path.parent

        temp_file = cwd / 'temp_data.pkl'
        temp_param_file = cwd / 'temp_params.json'
        
        if first_script:
            # Save data only for the first script
            with open(temp_file, 'wb') as f:
                pickle.dump(data, f)
        
        # Merge parameters and save them
        if temp_param_file.exists():
            with open(temp_param_file, 'r') as f:
                existing_params = json.load(f)
                parameters.update(existing_params)
        
        with open(temp_param_file, 'w') as f:
            json.dump(parameters, f)

        logging.debug("Running script: %s with parameters: %s", script_path, parameters)
        
        result = subprocess.run(
            ['python', str(script_path), str(temp_file), str(temp_param_file)],
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=os.environ
        )
        
        stdout_output = result.stdout
        stderr_output = result.stderr

        with open(log_file, 'a') as log:
            log.write(f"Output of {script_path}:\n")
            log.write(stdout_output)
            log.write(stderr_output)
            log.write("\n\n")
        
        if result.returncode != 0:
            logging.error(f"Script {script_path} returned non-zero exit status {result.returncode}")
            logging.error(stderr_output)
            raise subprocess.CalledProcessError(result.returncode, result.args, stdout_output, stderr_output)
        
        if temp_file.exists():
            with open(temp_file, 'rb') as f:
                data = pickle.load(f)
        
        return data, stdout_output, False  # False indicates no error
    
    except subprocess.CalledProcessError as e:
        error_message = f"Error running {script_path}: {e}\n{e.output}\n{e.stderr}"
        logging.error(error_message)
        with open(log_file, 'a') as log:
            log.write(error_message)
            log.write("\n\n")
        email_subject = f'FATAL ERROR: {script_path.name} HAS ENCOUNTERED A FATAL ERROR'
        email_body = f'Log file path: {log_file}'
        send_email(email_subject, email_body, os.getenv('EMAIL_error_to_email'))
        return None, error_message, True  # True indicates an error

    except Exception as e:
        error_message = f"Unexpected error running {script_path}: {e}"
        logging.error(error_message)
        with open(log_file, 'a') as log:
            log.write(error_message)
            log.write("\n\n")
        email_subject = f'FATAL ERROR: {script_path.name} HAS ENCOUNTERED A FATAL ERROR'
        email_body = f'Log file path: {log_file}'
        send_email(email_subject, email_body, os.getenv('EMAIL_error_to_email'))
        return None, error_message, True  # True indicates an error

def run_scripts_sequentially(scripts, parameters, log_file):
    data = None
    for index, script in enumerate(scripts):
        first_script = (index == 0)
        data, output, error = run_script(script, data, parameters, log_file, first_script=first_script)

        if error:
            print(f"Error: {script} encountered an error.")
            logging.error("Error: %s encountered an error.", script)
            sys.exit(1)  # Exit on error

        print(f"Output of {script}: {output}")

    return True

def numeric_sort_key(script_path):
    """Extracts the numeric part of the filename for sorting."""
    match = re.search(r'(\d+)', script_path.stem)
    return int(match.group(1)) if match else float('inf')

def main(base_dir=None, start_step=None, end_step=None, parameters=None):
    if not base_dir:
        print("Usage: python MIND.py <base_directory> [start_step] [end_step] [parameters...]")
        logging.error("Base directory is required.")
        sys.exit(1)

    base_dir = Path(base_dir).resolve()
    python_dir = base_dir / 'python'
    config_dir = base_dir / 'config'
    log_dir = base_dir.parents[1] / 'MIND_logs' / base_dir.name
    config_file = config_dir / 'config.ini'

    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file_path = log_dir / f"log_{timestamp}.txt"
    
    setup_logging(log_file_path)

    if not config_file.exists() or not config_file.is_file():
        raise FileNotFoundError(f"The config file {config_file} does not exist.")

    config = configparser.ConfigParser()
    config.read(config_file)

    parameters_dict = {
        "report_config_file_path": str(config_file),
    }

    for section in config.sections():
        for key, value in config.items(section):
            parameters_dict[key] = value

    # Only include scripts with a two-digit number at the end before .py
    scripts = [script for script in sorted(python_dir.glob('*.py'), key=numeric_sort_key) if re.search(r'\d{2}\.py$', script.name)]

    if start_step is not None:
        start_step = int(start_step)
        start_index = next((i for i, script in enumerate(scripts) if numeric_sort_key(script) >= start_step), None)
        if start_index is not None:
            scripts = scripts[start_index:]
        else:
            print(f"Error: No script found for starting step {start_step}")
            logging.error("No script found for starting step %s", start_step)
            sys.exit(1)

    if end_step is not None:
        end_step = int(end_step)
        end_index = next((i for i, script in enumerate(scripts) if numeric_sort_key(script) > end_step), None)
        if end_index is not None:
            scripts = scripts[:end_index]

    logging.info("Starting MIND script with base directory: %s, start step: %s, end step: %s, and parameters: %s", base_dir, start_step, end_step, parameters_dict)
    
    if scripts:
        success = run_scripts_sequentially(scripts, parameters_dict, log_file_path)
        if not success:
            print("Error encountered during script execution.")
    else:
        logging.error("No scripts found to execute.")
        print("No scripts found to execute.")

if __name__ == "__main__":
    base_directory = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else None
    start_step = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2].isdigit() else None
    end_step = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3].isdigit() else None
    parameters = sys.argv[4:] if len(sys.argv) > 4 else []

    if not base_directory or not base_directory.exists() or not base_directory.is_dir():
        print("Usage: python MIND.py <base_directory> [start_step] [end_step] [parameters...]")
        logging.error("Invalid base directory.")
        sys.exit(1)

    main(base_directory, start_step, end_step, parameters)
