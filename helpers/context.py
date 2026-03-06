import datetime
import pathlib
import os
from dotenv import load_dotenv

load_dotenv()


class DailyFilesContext:
    @staticmethod
    def get_today():
        return datetime.datetime.now()
    
    @staticmethod
    def get_yesterday():
        return DailyFilesContext.get_today() - datetime.timedelta(days = 1)

    #dates in different string formats
    @staticmethod
    def daily_file_dir_date():
        return DailyFilesContext.get_yesterday().strftime(format = "%m%d%Y")

    
    @staticmethod
    def transaction_file_date():
        return DailyFilesContext.get_today().strftime(format = "%Y%m%d")
    
    @staticmethod
    def open_backorders_file_date():
        return DailyFilesContext.get_today().strftime(format = "%d%m%Y")
    
    @staticmethod
    def today_date_sring():
        return DailyFilesContext.get_today().strftime("%d%m%Y")
    
    #names
    @staticmethod
    def transaction_file_name():
        return f"TransactionFile{DailyFilesContext.transaction_file_date()}.txt"
    
    @staticmethod
    def open_backorders_file_name():
        return f"open_backorders_{DailyFilesContext.open_backorders_file_date()}.csv"

    @staticmethod
    def fileserver_base() -> str:
        if os.getenv("FILESHARE_VERSION") == "NEW":
            return r"\\tuttlepub.com\fileshare"
        return r"\\tutpub3"

    #paths
    @staticmethod
    def daily_files_path():
        return pathlib.Path(DailyFilesContext.fileserver_base()).joinpath("VOL2", "FOXPRO", "TestFiles", DailyFilesContext.daily_file_dir_date())
    
    @staticmethod
    def daily_files_logs_path():
        return DailyFilesContext.daily_files_path().joinpath("logs")

    #files
    @staticmethod
    def trainsaction_file_path():
        return DailyFilesContext.daily_files_logs_path().joinpath(DailyFilesContext.transaction_file_name())
    
    @staticmethod
    def credit_detail_file():
        return DailyFilesContext.daily_files_path().joinpath("CR_Detail.xls")
    
    @staticmethod
    def revenue_detail_file():
        return DailyFilesContext.daily_files_path().joinpath("RV_Detail.xls")
