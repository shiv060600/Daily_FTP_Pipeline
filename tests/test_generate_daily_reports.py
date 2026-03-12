import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
from contextlib import contextmanager

from logic.generate_daily_reports import generate_daily_reports

TEST_OUTPUT_PATH = r"\\tutpub5\Upgrading_Database_Reporting_Systems\CODE_TESTS"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db_mock(report_names: list[str], df: pd.DataFrame | None = None):
    """Return a _mock_get_db context manager that yields the given report names."""
    @contextmanager
    def _mock_get_db():
        engine = MagicMock()
        conn = MagicMock()
        conn.execute.return_value = [(name,) for name in report_names]
        begin_cm = MagicMock()
        begin_cm.__enter__ = MagicMock(return_value=conn)
        begin_cm.__exit__ = MagicMock(return_value=False)
        engine.begin.return_value = begin_cm
        yield engine
    return _mock_get_db


SAMPLE_DF = pd.DataFrame({
    "REASONCODE": ["CC", "OH"],
    "WHS": ["IPS", "IPS"],
    "EAN": ["9781234567890", "9780987654321"],
    "TITLE": ["Test Book One", "Test Book Two"],
    "QTY": [5, 3],
})


# ---------------------------------------------------------------------------
# Unit tests — DB mocked, real file generation to TEST_OUTPUT_PATH
# ---------------------------------------------------------------------------

class TestGenerateDailyReportsUnit:

    def test_creates_test_reports_directory(self, mock_reports_db):
        generate_daily_reports(path=TEST_OUTPUT_PATH)
        assert (Path(TEST_OUTPUT_PATH) / "Test_Reports").is_dir()

    def test_creates_excel_file_in_expected_directory(self, mock_reports_db):
        generate_daily_reports(path=TEST_OUTPUT_PATH)
        expected = Path(TEST_OUTPUT_PATH) / "Test_Reports" / "INV_ADJ_CC_IPS.xlsx"
        assert expected.exists(), f"Expected xlsx file at {expected}"

    def test_creates_pdf_file_in_expected_directory(self, mock_reports_db):
        generate_daily_reports(path=TEST_OUTPUT_PATH)
        expected = Path(TEST_OUTPUT_PATH) / "Test_Reports" / "INV_ADJ_CC_IPS.pdf"
        assert expected.exists(), f"Expected pdf file at {expected}"

    def test_returns_passed(self, mock_reports_db):
        result = generate_daily_reports(path=TEST_OUTPUT_PATH)
        assert result == "Passed"

    def test_raises_when_no_available_reports(self):
        with patch("logic.generate_daily_reports.get_db", _make_db_mock([])):
            with pytest.raises(RuntimeError, match="No available reports"):
                generate_daily_reports(path=TEST_OUTPUT_PATH)

    def test_raises_when_report_df_is_empty(self):
        empty_df = pd.DataFrame(columns=["REASONCODE", "WHS", "EAN", "TITLE", "QTY"])
        with patch("logic.generate_daily_reports.get_db", _make_db_mock(["INV_ADJ_CC_IPS"])), \
             patch("pandas.read_sql_query", return_value=empty_df):
            with pytest.raises(RuntimeError, match="marked as having data"):
                generate_daily_reports(path=TEST_OUTPUT_PATH)

    def test_unknown_report_is_skipped_and_returns_passed(self):
        """Reports not in REPORT_SQL should be silently skipped."""
        with patch("logic.generate_daily_reports.get_db", _make_db_mock(["NOT_A_REAL_REPORT"])):
            result = generate_daily_reports(path=TEST_OUTPUT_PATH)
        assert result == "Passed"

    def test_multiple_reports_each_produce_excel_and_pdf(self):
        with patch("logic.generate_daily_reports.get_db",
                   _make_db_mock(["INV_ADJ_CC_IPS", "INV_ADJ_CC_ING"])), \
             patch("pandas.read_sql_query", return_value=SAMPLE_DF.copy()):
            generate_daily_reports(path=TEST_OUTPUT_PATH)

        test_reports = Path(TEST_OUTPUT_PATH) / "Test_Reports"
        assert (test_reports / "INV_ADJ_CC_IPS.xlsx").exists()
        assert (test_reports / "INV_ADJ_CC_ING.xlsx").exists()
        assert (test_reports / "INV_ADJ_CC_IPS.pdf").exists()
        assert (test_reports / "INV_ADJ_CC_ING.pdf").exists()


# ---------------------------------------------------------------------------
# Integration tests — real DB, real file generation, all output to TEST_OUTPUT_PATH
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestGenerateDailyReportsIntegration:

    def test_integration_runs_and_returns_passed(self):
        """
        Full run against the real DB.
        Output goes to TEST_OUTPUT_PATH\\Test_Reports — never to the production daily files path.
        Requires valid DB credentials in .env.
        """
        result = generate_daily_reports(path=TEST_OUTPUT_PATH)
        assert result == "Passed"

    def test_integration_creates_xlsx_files_at_test_output_path(self):
        generate_daily_reports(path=TEST_OUTPUT_PATH)
        test_reports = Path(TEST_OUTPUT_PATH) / "Test_Reports"
        assert test_reports.is_dir(), f"Test_Reports dir not created at {test_reports}"
        assert len(list(test_reports.glob("*.xlsx"))) > 0, "No xlsx files produced"

    def test_integration_creates_pdf_files_at_test_output_path(self):
        generate_daily_reports(path=TEST_OUTPUT_PATH)
        test_reports = Path(TEST_OUTPUT_PATH) / "Test_Reports"
        assert len(list(test_reports.glob("*.pdf"))) > 0, "No pdf files produced"
