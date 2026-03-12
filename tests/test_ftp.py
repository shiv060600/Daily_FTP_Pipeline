import pytest
import datetime
from pathlib import Path

from logic.FTP import Name_Creator

TEST_OUTPUT_PATH = r"\\tutpub5\Upgrading_Database_Reporting_Systems\CODE_TESTS"


# ---------------------------------------------------------------------------
# Unit tests — Name_Creator has no side effects, no mocking needed
# ---------------------------------------------------------------------------

class TestNameCreator:

    def setup_method(self):
        self.day = datetime.datetime(2025, 3, 11)
        self.fday = self.day + datetime.timedelta(days=1)  # Name_Creator adds 1 day internally

    def test_cdt_pattern_starts_with_fday_mmdd(self):
        result = Name_Creator("CDT", self.day)
        assert result.startswith(self.fday.strftime("%m%d"))

    def test_cdt_pattern_ends_with_cdt_extension(self):
        result = Name_Creator("CDT", self.day)
        assert result.endswith(".CDT")

    def test_cdp_pattern_starts_with_fday_mmdd(self):
        result = Name_Creator("CDP", self.day)
        assert result.startswith(self.fday.strftime("%m%d"))

    def test_cdp_pattern_ends_with_cdp_extension(self):
        result = Name_Creator("CDP", self.day)
        assert result.endswith(".CDP")

    def test_trans_name_contains_fday_yyyymmdd(self):
        result = Name_Creator("Trans", self.day)
        assert self.fday.strftime("%Y%m%d") in result

    def test_trans_name_starts_with_transactionfile(self):
        result = Name_Creator("Trans", self.day)
        assert result.startswith("TransactionFile")

    def test_folder_name_uses_day_not_fday(self):
        result = Name_Creator("Folder", self.day)
        assert result == self.day.strftime("%m%d%Y")

    def test_unknown_type_raises_value_error(self):
        with pytest.raises(ValueError):
            Name_Creator("UNKNOWN", self.day)


# ---------------------------------------------------------------------------
# Integration tests — require fileserver access and valid credentials in .env
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestFTPIntegration:

    def test_daily_folder_setup_creates_directory(self):
        """
        Creates the daily folder on the fileserver for a fixed past date.
        Uses a safe historical date so it cannot collide with today's production run.
        Requires network access to the fileserver defined in DailyFilesContext.
        """
        import logic.FTP as FTP
        from helpers.context import DailyFilesContext
        import os

        safe_past_day = datetime.datetime(2020, 1, 15)
        FTP.Daily_Folder_Setup(safe_past_day)
        expected = (
            DailyFilesContext.fileserver_base()
            + "\\vol2\\FOXPRO\\TestFiles\\"
            + Name_Creator("Folder", safe_past_day)
        )
        assert os.path.exists(expected), f"Expected directory not created: {expected}"

