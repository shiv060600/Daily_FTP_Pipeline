import ftplib, datetime, os, shutil, time, fnmatch, getpass
import logging
from email.message import EmailMessage
import smtplib
from helpers.ENV import CREDS
from helpers.ENV import EMAIL_CONFIG
import chardet

def send_warning_email():
    message = EmailMessage()
    message['Subject'] = 'Warning, dailyfiles data from ingram may be incorrect'
    message['From'] = EMAIL_CONFIG['EMAIL_USER']
    message['To'] = EMAIL_CONFIG['EMAIL_TO']

    with smtplib.SMTP(EMAIL_CONFIG['SMTP_SERVER'], EMAIL_CONFIG['SMTP_PORT']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['EMAIL_USER'], EMAIL_CONFIG['EMAIL_PASSWORD'])
            server.send_message(message)
            logging.info("Email sent successfully")

def FTP_pull(day):
    """
    Downloads the latest CDT, CDP, and Transaction files from the Ingram Publisher Services FTP server.
    
    Args:
        day (datetime): The date to process files for
        
    Returns:
        dict: Dictionary containing the names of downloaded CDT and CDP files
    """
    logging.info("Starting FTP_pull function")
    conn = ftplib.FTP('ftp.ingrampublisherservices.com')
    curr_date = datetime.datetime.now().strftime("%Y%m%d")
    try:
        dirPath = "\\\\TUTPUB3\\vol2\\FOXPRO\\TestFiles\\" + Name_Creator("Folder", day)
        log_dir_path = "\\\\TUTPUB3\\vol2\\FOXPRO\\TestFiles\\" + Name_Creator("Folder", day) + "\\logs"
        logging.info(f"Directory path for files: {dirPath}")
        logging.info(f"Connecting to FTP as user a20V0190")
        
        conn.login(CREDS['USER'], CREDS['PASS'])
        conn.cwd('outgoing')
        names = conn.nlst() 
        logging.info(f"Found {len(names)} files in FTP directory")

        cdt_pattern = Name_Creator("CDT", day)
        cdp_pattern = Name_Creator("CDP", day)
        logging.info(f"Looking for CDT files matching: {cdt_pattern}")
        logging.info(f"Looking for CDP files matching: {cdp_pattern}")
        
        cdtNames = fnmatch.filter(names, cdt_pattern)
        cdpNames = fnmatch.filter(names, cdp_pattern)
        
        logging.info(f"Found {len(cdtNames)} CDT files matching pattern")
        logging.info(f"Found {len(cdpNames)} CDP files matching pattern")

        latest_cdtname = None
        latest_cdpname = None
        latest_cdttime = None
        latest_cdptime = None

        for cdt in cdtNames:
            cdttime = conn.voidcmd("MDTM " + cdt)
            logging.info(f"CDT file {cdt} has timestamp {cdttime}")
            if (latest_cdttime is None) or (cdttime > latest_cdttime):
                latest_cdtname = cdt
                latest_cdttime = cdttime
                logging.info(f"New latest CDT: {latest_cdtname}")

        for cdp in cdpNames:
            cdptime = conn.voidcmd("MDTM " + cdp)
            logging.info(f"CDP file {cdp} has timestamp {cdptime}")
            if (latest_cdptime is None) or (cdptime > latest_cdptime):
                latest_cdpname = cdp
                latest_cdptime = cdptime
                logging.info(f"New latest CDP: {latest_cdpname}")

        logging.info(f"Selected latest CDT: {latest_cdtname}")
        logging.info(f"Selected latest CDP: {latest_cdpname}")

        trans_name = Name_Creator("Trans", day)
        logging.info(f"Transaction file name: {trans_name}")
        
        """
        Download all the files to dailyfiles dir for use
        """
        logging.info(f"Downloading {latest_cdtname} to {dirPath}")
        with open(dirPath + "\\" + str(latest_cdtname), 'wb') as CDT:
            conn.retrbinary("RETR " + str(latest_cdtname), CDT.write)
        
        logging.info(f"Downloading {latest_cdpname} to {dirPath}")
        with open(dirPath + "\\" + str(latest_cdpname), 'wb') as CDP:
            conn.retrbinary("RETR " + str(latest_cdpname), CDP.write)
        
        logging.info(f"Creating INPRO.CDP from {latest_cdpname}")
        with open(dirPath + "\\INPRO.CDP", 'wb') as INPRO:
            conn.retrbinary("RETR " + str(latest_cdpname), INPRO.write)
        
        logging.info(f"Downloading {trans_name}")
        with open(dirPath + "\\" + trans_name, 'wb') as Trans:
            conn.retrbinary("RETR " + trans_name, Trans.write)

        """
        Copy all the files for retrbinary 
        """

        logging.info(f"Donloading {latest_cdtname} to logs")
        with open(os.path.join(dirPath,f'logs\\{latest_cdtname}'),'wb') as CDTCOPY:
            conn.retrbinary(f"RETR {latest_cdtname}", CDTCOPY.write)

        logging.info(f"Downdloading {latest_cdpname} to logs")
        with open(os.path.join(dirPath,f"logs\\{latest_cdpname}"),'wb') as CDPCOPY:
            conn.retrbinary(f"RETR {latest_cdpname}",CDPCOPY.write)

        logging.info(f"Downloading {trans_name} to logs")
        with open(os.path.join(dirPath,f"logs\\{trans_name}"),'wb') as TRANSFILE:
            conn.retrbinary(f"RETR {trans_name}",TRANSFILE.write)

        
        # All downloads complete we now must delete the files
        logging.info("All files downloaded successfully, now deleting from FTP server")
        
        conn.quit()
        logging.info("FTP connection closed")

        filenames = dict()
        filenames["CDT"] = latest_cdtname
        filenames["CDP"] = latest_cdpname
        # Check the dates to make sure Ingram didn't make a mistake
        # letest cdp
        date_problems = False
        with open(dirPath + "\\" + str(latest_cdpname)) as f:
            for i in range(10):
                line = f.readline().strip()
                line_list = line.split(',')
                if line_list[1] != curr_date:
                    date_problems = True
        
        with open(dirPath + "\\" + str(latest_cdtname)) as f:
            for i in range(10):
                line = f.readline().strip()
                line_list = line.split(',')
                if line_list[1] != curr_date:
                    date_problems = True
        
        if date_problems:
            send_warning_email()

        logging.info(f"Returning filenames dictionary: {filenames}")
        return filenames
    except ftplib.all_errors as e:
        logging.error(f"FTP error: {e}")
        conn.quit()
        raise
    except Exception as e:
        logging.error(f"Unexpected error in FTP_pull: {e}")
        if conn:
            conn.quit()
        raise

def File_Copy(names, day):
    """
    Copies downloaded files to the Daily Files directory and renames them to standard names.
    
    Args:
        names (dict): Dictionary containing CDT and CDP file names
        day (datetime): The date being processed
    """
    logging.info("Starting File_Copy function")
    dirPath = "\\\\TUTPUB3\\vol2\\FOXPRO\\TestFiles\\" + Name_Creator("Folder", day)
    dest = "\\\\TUTPUB3\\vol2\\FOXPRO\\TestFiles\\Daily Files"
    
    logging.info(f"Source directory: {dirPath}")
    logging.info(f"Destination directory: {dest}")
    logging.info(f"Running as user: {getpass.getuser()}")
    
    # Verify directories exist
    logging.info(f"Source directory exists: {os.path.exists(dirPath)}")
    logging.info(f"Destination directory exists: {os.path.exists(dest)}")
    
    # Create destination if needed
    if not os.path.exists(dest):
        try:
            os.makedirs(dest)
            logging.info(f"Created destination directory: {dest}")
        except Exception as e:
            logging.error(f"Failed to create destination directory: {e}")
            raise
    
    try:
        # List files
        src = os.listdir(dirPath)
        logging.info(f"Files in source directory: {src}")
        
        # Check for target files
        cdt_in_src = names["CDT"] in src
        cdp_in_src = names["CDP"] in src
        trans_in_src = Name_Creator("Trans", day) in src
        logging.info(f"CDT file found in source: {cdt_in_src}")
        logging.info(f"CDP file found in source: {cdp_in_src}")
        logging.info(f"Trans file found in source: {trans_in_src}")



        
        # Clean destination directory
        if os.path.exists(dest):
            rensrc = os.listdir(dest)
            logging.info(f"Files in destination before cleanup: {rensrc}")
            for f in rensrc:
                try:
                    os.remove(dest + "\\" + f)
                    logging.info(f"Removed file: {dest}\\{f}")
                except Exception as e:
                    logging.error(f"Failed to remove {f}: {e}")
        
        # Copy files
        for fileName in src:
            fullfilename = os.path.join(dirPath, fileName)
            try:
                shutil.copy(fullfilename, dest)
                logging.info(f"Copied {fileName} to destination")
            except Exception as e:
                logging.error(f"Failed to copy {fileName}: {e}")
        
        # Verify files were copied
        copied_files = os.listdir(dest)
        logging.info(f"Files in destination after copy: {copied_files}")
        
        # Rename files
        for files in copied_files:
            try:
                if files == names["CDT"]:
                    os.rename(dest + "\\" + names["CDT"], dest + "\\" + 'IPS_INV.CDT')
                    logging.info(f"Renamed {names['CDT']} to IPS_INV.CDT")
                elif files == names["CDP"]:
                    os.rename(dest + "\\" + names["CDP"], dest + "\\" + 'LOCKED.CDP')
                    logging.info(f"Renamed {names['CDP']} to LOCKED.CDP")
                elif files == Name_Creator("Trans", day):
                    os.rename(dest + "\\" + Name_Creator("Trans", day), dest + "\\" + 'IPS_DAILY_NO_LINE_NUM.TXT')
                    logging.info(f"Renamed {Name_Creator('Trans', day)} to IPS_DAILY_NO_LINE_NUM.TXT")
                else:
                    logging.info(f"No rename rule for {files}")
            except Exception as e:
                logging.error(f"Failed to rename {files}: {e}")
        
        # Final verification
        final_files = os.listdir(dest)
        logging.info(f"Final files in destination: {final_files}")
        logging.info(f"IPS_INV.CDT exists in destination: {'IPS_INV.CDT' in final_files}")
        if 'IPS_INV.CDT' in final_files:
            file_size = os.path.getsize(dest + "\\IPS_INV.CDT")
            logging.info(f"IPS_INV.CDT size: {file_size} bytes")
            logging.info(f"IPS_INV.CDT permissions: {oct(os.stat(dest + '\\IPS_INV.CDT').st_mode)}")
        
    except PermissionError as e:
        logging.error(f"PermissionError in File_Copy: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred in File_Copy: {e}")
        raise

def Daily_Folder_Setup(day):
    """
    Creates and sets up the daily folder for file processing.
    
    Args:
        day (datetime): The date to create the folder for
        
    Returns:
        int: 0 if successful
    """
    logging.info("Starting Daily_Folder_Setup function")
    dirPath = "\\\\TUTPUB3\\vol2\\FOXPRO\\TestFiles\\" + Name_Creator("Folder", day)
    logging.info(f"Setting up directory: {dirPath}")
    logging.info(f"Running as user: {getpass.getuser()}")
    
    try:
        if os.path.exists(dirPath):
            logging.info(f"Directory already exists, cleaning it up")
            for f in os.listdir(dirPath):
                full_path = os.path.join(dirPath, f)
                if os.path.isdir(full_path):
                    logging.info(f"Removing subdirectory: {full_path}")
                    shutil.rmtree(full_path)
                else:
                    logging.info(f"Removing file: {full_path}")
                    os.remove(full_path)
            logging.info(f"Removing directory: {dirPath}")
            shutil.rmtree(dirPath)
        
        logging.info(f"Creating directory: {dirPath}")
        os.mkdir(dirPath)
        logPath = os.path.join(dirPath,'logs')
        if not os.path.exists(logPath):
            os.mkdir(logPath)
            logging.info(f"Logs subdirectory created: {logPath}")
        logging.info(f"Directory created successfully")
        
        # Verify directory was created
        if os.path.exists(dirPath):
            logging.info(f"Verified directory exists: {dirPath}")
            logging.info(f"Directory permissions: {oct(os.stat(dirPath).st_mode)}")
        else:
            logging.error(f"Failed to create directory: {dirPath}")
            
    except PermissionError as e:
        logging.error(f"PermissionError in Daily_Folder_Setup: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred in Daily_Folder_Setup: {e}")
        raise
    return 0

def Name_Creator(NameType, day):
    """
    Creates standardized file names based on type and date.
    
    Args:
        NameType (str): Type of name to create ('CDT', 'CDP', 'Trans', or 'Folder')
        day (datetime): The date to use in the name
        
    Returns:
        str: Generated file name
    """
    logging.info(f"Creating name of type {NameType} for day {day}")
    try:
        fday = day + datetime.timedelta(days=1)
        logging.info(f"Using date: {fday} (day + 1)")
        
        if NameType == "CDT":
            result = fday.strftime("%m%d") + "????.CDT"
        elif NameType == "CDP":
            result = fday.strftime("%m%d") + "????.CDP"
        elif NameType == "Trans":
            result = "TransactionFile" + fday.strftime("%Y%m%d") + ".txt"
        elif NameType == "Folder":
            result = day.strftime("%m%d%Y")
        else:
            logging.error(f"Unknown name type: {NameType}")
            raise ValueError(f"Error unknown name type: {NameType}")
            
        logging.info(f"Generated name: {result}")
        return result
    except Exception as e:
        logging.error(f"Error in Name_Creator: {e}")
        raise

def File_Fixes(names, day):
    """
    Processes downloaded files to add required formatting and remove special characters.
    
    Args:
        names (dict): Dictionary containing CDT and CDP file names
        day (datetime): The date being processed
    """
    logging.info("Starting File_Fixes function")
    dirPath = "\\\\TUTPUB3\\vol2\\FOXPRO\\TestFiles\\" + Name_Creator("Folder", day)
    logging.info(f"Working directory: {dirPath}")
    logging.info(f"Running as user: {getpass.getuser()}")
    
    CDT = dirPath + "\\" + names["CDT"]
    CDTTemp = dirPath + "\\" + "IPS_INV.CDT"
    CDP = dirPath + "\\" + names["CDP"]
    CDPTemp = dirPath + "\\" + "Locked.CDP"
    Trans = dirPath + "\\" + Name_Creator("Trans", day)
    TransTemp = dirPath + "\\" + "IPS_DALY.txt"
    
    logging.info(f"Source CDT: {CDT}")
    logging.info(f"Destination CDT: {CDTTemp}")
    logging.info(f"Source CDP: {CDP}")
    logging.info(f"Destination CDP: {CDPTemp}")
    logging.info(f"Source Trans: {Trans}")
    logging.info(f"Destination Trans: {TransTemp}")

    
    try:
        logging.info(f"Processing CDT file")
        logging.info(f"Source CDT exists: {os.path.exists(CDT)}")
        if os.path.exists(CDT):
            with open(CDT, 'r') as read_CDT, open(CDTTemp, 'w') as write_CDT:
                logging.info(f"Adding newline to beginning of CDT file")
                write_CDT.write("\n")
                for line in read_CDT:
                    write_CDT.write(line)
            logging.info(f"CDT processing complete")
            logging.info(f"Output CDT exists: {os.path.exists(CDTTemp)}")
            if os.path.exists(CDTTemp):
                logging.info(f"Output CDT size: {os.path.getsize(CDTTemp)} bytes")

        logging.info(f"Processing CDP file")
        logging.info(f"Source CDP exists: {os.path.exists(CDP)}")
        if os.path.exists(CDP):
            with open(CDP, 'r') as read_CDP, open(CDPTemp, 'w') as write_CDP:
                logging.info(f"Adding newline to beginning of CDP file")
                write_CDP.write("\n")
                for line in read_CDP:
                    write_CDP.write(line)
            logging.info(f"CDP processing complete")
            logging.info(f"Output CDP exists: {os.path.exists(CDPTemp)}")
            if os.path.exists(CDPTemp):
                logging.info(f"Output CDP size: {os.path.getsize(CDPTemp)} bytes")

        logging.info(f"Processing Trans file")
        logging.info(f"Source Trans exists: {os.path.exists(Trans)}")
        Trans = os.path.join(dirPath,Name_Creator("Trans",day))

        if os.path.exists(Trans):
            try:
                with open(Trans, 'r', encoding='utf-8') as read_Trans, open(TransTemp, 'w') as write_Trans:
                    data = read_Trans.read()
                    logging.info(f"Removing special characters from Trans file")
                    data = data.replace("\"", "")
                    data = data.replace("'", "")
                    data = data.replace(",", "")
                    write_Trans.write(data)
            except UnicodeDecodeError:
                logging.warning("UTF-8 decode failed, trying cp1252 (ANSI)...")
                with open(Trans, 'r', encoding='cp1252', errors='replace') as read_Trans, open(TransTemp, 'w') as write_Trans:
                    data = read_Trans.read()
                    logging.info(f"Removing special characters from Trans file")
                    data = data.replace("\"", "")
                    data = data.replace("'", "")
                    data = data.replace(",", "")
                    write_Trans.write(data)
            logging.info(f"Trans processing complete")
            logging.info(f"Output Trans exists: {os.path.exists(TransTemp)}")
            if os.path.exists(TransTemp):
                logging.info(f"Output Trans size: {os.path.getsize(TransTemp)} bytes")
                
    except PermissionError as e:
        logging.error(f"PermissionError in File_Fixes: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred in File_Fixes: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("FTP module test run")