import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from contextlib import contextmanager
from helpers.context import DailyFilesContext

TEST_OUTPUT_PATH = r"\\tutpub5\Upgrading_Database_Reporting_Systems\CODE_TESTS"

SAMPLE_STANDARD_DF = pd.DataFrame({
    "REASONCODE": ["CC", "CC"],
    "WHS": ["IPS", "IPS"],
    "EAN": ["9781234567890", "9780987654321"],
    "TITLE": ["Test Book One", "Test Book Two"],
    "QTY": [5, 3],
})


@pytest.fixture
def test_output_path():
    """Network path for test-generated output files. Does not touch daily production files."""
    return TEST_OUTPUT_PATH


@pytest.fixture
def mocked_daily_files_path():
    return DailyFilesContext.daily_files_path().joinpath("Test_Reports")


@pytest.fixture
def mock_reports_db():
    """
    Mocks only DB access (get_db + pd.read_sql_query) with sample INV_ADJ_CC_IPS data.
    File generation (xlsx, pdf) runs for real — output goes to TEST_OUTPUT_PATH.
    """
    @contextmanager
    def _mock_get_db():
        engine = MagicMock()
        conn = MagicMock()
        conn.execute.return_value = [("INV_ADJ_CC_IPS",)]
        begin_cm = MagicMock()
        begin_cm.__enter__ = MagicMock(return_value=conn)
        begin_cm.__exit__ = MagicMock(return_value=False)
        engine.begin.return_value = begin_cm
        yield engine

    with patch("logic.generate_daily_reports.get_db", _mock_get_db), \
         patch("pandas.read_sql_query", return_value=SAMPLE_STANDARD_DF.copy()):
        yield
