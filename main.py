import datetime
import getpass  
import pyodbc   
import csv
import ftplib, os, shutil, time, fnmatch
import logic.FTP as FTP
import logic.FIX as FIX
import logging
import smtplib
from email.message import EmailMessage
from helpers.SQL import SQLrun
from helpers.db_conn import get_db
from helpers.context import DailyFilesContext
from logic.sage_uploads import generate_sage_uploads
from pyodbc import *
import pandas as pd
import sys
from helpers.ENV import EMAIL_CONFIG

def send_failure_email(error_message):
    """Send email notification when daily file processing fails"""
    try:
        msg = EmailMessage()
        msg['Subject'] = "Daily File FAILED - Action Required"
        msg['From'] = EMAIL_CONFIG['EMAIL_USER']
        msg['To'] = 'sbhutani@tuttlepublishing.com'
        msg.set_content(f"Daily file has FAILED please check\n\nError Details:\n{error_message}")
        
        with smtplib.SMTP(EMAIL_CONFIG['SMTP_SERVER'], EMAIL_CONFIG['SMTP_PORT']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['EMAIL_USER'], EMAIL_CONFIG['EMAIL_PASSWORD'])
            server.send_message(msg)
            logging.info(f"Failure notification email sent to sbhutani@tuttlepublishing.com")
    except Exception as e:
        logging.error(f"Failed to send failure notification email: {e}")

def setup_logging():
    daily_file_logs_dir = DailyFilesContext.daily_files_logs_path()

    today = datetime.datetime.now().strftime("%Y-%m-%d")

    log_file = os.path.join(r"H:\Upgrading_Database_Reporting_Systems\DAILY_FILES_PIPELINE\logs", f'daily_run_{today}.log')


    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file,mode='w'),
            logging.StreamHandler()  
    ])


def run_daily_file():
    logging.info("Starting daily file run")
    # Create a proper datetime object instead of a string
    day_obj = datetime.datetime.now() + datetime.timedelta(days=-1)
    # Use the datetime object for FTP functions that need it
    day_str = day_obj.strftime('%m/%d/%y')
    
    logging.info(f"Processing date: {day_str}")
    
    try:
        # Pass the datetime object instead of string
        FTP.Daily_Folder_Setup(day_obj)
        logging.info("Folder setup complete")
        
        names = FTP.FTP_pull(day_obj)
        logging.info(f"FTP pull complete: {names}")
        
        FTP.File_Copy(names, day_obj)
        logging.info("File copy complete")
        
        FTP.File_Fixes(names, day_obj)
        logging.info("File fixes complete")
        
        FIX.Fixes()
        logging.info("Additional fixes complete")
        
        # Add verification before calling SQLrun()
        dest_dir = "\\\\TUTPUB3\\vol2\\FOXPRO\\TestFiles\\Daily Files"
        required_files = ["IPS_INV.CDT", "LOCKED.CDP", "IPS_DAILY_NO_LINE_NUM.TXT"]
        
        missing_files = []
        for file in required_files:
            file_path = os.path.join(dest_dir, file)
            if not os.path.exists(file_path):
                missing_files.append(file)
                logging.error(f"Required file missing: {file}")
            else:
                # Check file size to ensure it's not empty
                file_size = os.path.getsize(file_path)
                if file_size == 0:
                    missing_files.append(f"{file} (empty)")
                    logging.error(f"File {file} exists but is empty (0 bytes)")
                else:
                    logging.info(f"Verified {file} exists with size {file_size} bytes")
        if missing_files:
            error_msg = f"Cannot run SQL job - missing required files: {missing_files}"
            logging.error(error_msg)
            logging.error(f"Files in destination directory: {os.listdir(dest_dir)}")
            send_failure_email(error_msg)
            return
        # Only run SQL job if all files exist and have content
        logging.info("All required files verified in Daily Files directory")
        logging.info("About to run SQL Job")
        SQLrun()
        logging.info("SQL Job function called")
    except Exception as e:
        error_msg = f"Error in processing: {e}"
        logging.error(error_msg, exc_info=True)
        send_failure_email(error_msg)
    
#send emails at the end
def send_emails():
    dir_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%m%d%Y")
    daily_files_folder_path = "\\\\TUTPUB3\\vol2\\FOXPRO\\TestFiles\\" + dir_date
    if os.path.exists(daily_files_folder_path):
        logging.info(f"Folder exsist for day {dir_date} starting send email proccess")
    else:
        logging.info(f"Folder does not exist for day {dir_date} cannot send email") 

    reports_path = os.path.join(daily_files_folder_path, 'Reports')
    reports = os.listdir(reports_path)
    reports_full_path = [os.path.join(reports_path,report) for report in reports]

    #make email message 
    msg = EmailMessage()
    msg['Subject'] = f"Daily Files {dir_date}"
    msg['From'] = EMAIL_CONFIG['EMAIL_USER']
    msg['To'] = EMAIL_CONFIG['EMAIL_TO']

    #attach pdfs
    for file_path in reports_full_path:
        try:
            with open(file_path, 'rb') as f:
                file_data = f.read()
                file_name = os.path.basename(file_path)
            msg.add_attachment(file_data, maintype='application', 
                              subtype='octet-stream', filename=file_name)
            logging.info(f"Attached file: {file_name}")
        except Exception as e:
            logging.error(f"Failed to attach {file_path}: {e}")
    try:
        with smtplib.SMTP(EMAIL_CONFIG['SMTP_SERVER'], EMAIL_CONFIG['SMTP_PORT']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['EMAIL_USER'], EMAIL_CONFIG['EMAIL_PASSWORD'])
            server.send_message(msg)
            logging.info("Email sent successfully")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
    
if __name__ == "__main__":
    try:
        setup_logging()
        logging.info("----- New execution started -----")
        logging.info(f"Running as user: {getpass.getuser()}")
        logging.info(f"Current directory: {os.getcwd()}")
        
        run_daily_file()
        #cant send emails until reports are moved into Reports folder after SQL server JOB
        dir_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%m%d%Y")
        report_folder_path = os.path.join("\\\\TUTPUB3\\vol2\\FOXPRO\\TestFiles\\",dir_date,"Reports")
        time_waited = 0
        interval = 20
        time_out = 1800
        while not os.path.exists(report_folder_path):
            if time_waited >= time_out:
                error_msg = f'Timed out waiting for reports folder to be created after {time_out} seconds'
                logging.error(error_msg)
                send_failure_email(error_msg)
                sys.exit(1)
            logging.info(f"at {time_waited}, {report_folder_path} still does not exist")
            time.sleep(interval)
            time_waited += interval
            
        time.sleep(30)
        #finally send emails of the generated pdf reports.
        send_emails()

        """
        bhuvan wants copied files from the db, we can easily db_conn read them and excel write them into sage uploads without messing around with the current files.
        """
        generate_sage_uploads()
        

        #add extra analysis of excel files
        logging.info("----- Execution completed -----")
    except ImportError as e:
        error_msg = f"IMPORT ERROR: {e}"
        logging.error(error_msg)
        send_failure_email(error_msg)
    except Exception as e:
        error_msg = f"EXECUTION ERROR: {e}"
        logging.error(error_msg)
        send_failure_email(error_msg)
