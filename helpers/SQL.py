import pyodbc
from pyodbc import *
import getpass
import logging 
from helpers.ENV import SQL_CONFIG

def SQLrun():
    logging.info(f"SQLrun started by user: {getpass.getuser()}")
    c = None
    try:
        c = connect(SQL_CONFIG['CONNECTION_STRING'])
        cursor = c.cursor()
        logging.info("SQL Connection successful")
        cursor.execute('''EXEC msdb.dbo.sp_start_job N'Daily Rerun' ''')
        c.commit()
        logging.info("SQL Job Start Command Executed")
        logging.info("SQL Job Started Successfully")
    except pyodbc.Error as pyodbc_err:
        logging.error(f"pyodbc Error in SQLrun: {pyodbc_err}")
    except Exception as e:
        logging.error(f"General Error in SQLrun: {e}")
    finally:
        if c:
            c.close()
            logging.info("Database connection closed")
