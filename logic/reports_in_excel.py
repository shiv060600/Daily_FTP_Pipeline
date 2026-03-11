import logging
from pathlib import Path

import pandas as pd
from helpers.context import DailyFilesContext
from helpers.db_conn import get_db
from sqlalchemy import text

CC_REPORTS = {"INV_ADJ_CC_IPS", "INV_ADJ_CC_ING"}

REPORT_SQL : dict[str,str] = {
    "INV_ADJ_CC_IPS": """
        SELECT IPS.Acttype AS REASONCODE, IPS.WHS, CAST(IPS.EAN AS Char(24)) AS EAN,
               I.[DESC] AS TITLE, IPS.Qty AS QTY
        FROM IPS.dbo.IPS_INV AS IPS
        LEFT JOIN TUTLIV.dbo.ICITEM I ON TRIM(I.ITEMNO) = TRIM(CAST(IPS.EAN AS Char(24)))
        WHERE IPS.WHS = 'IPS' AND IPS.Acttype = 'CC'
    """,
    "INV_ADJ_OH_ING": """
        SELECT IPS.Acttype AS REASONCODE, IPS.WHS, CAST(IPS.EAN AS Char(24)) AS EAN,
               I.[DESC] AS TITLE, IPS.Qty AS QTY
        FROM IPS.dbo.IPS_INV AS IPS
        LEFT JOIN TUTLIV.dbo.ICITEM I ON TRIM(I.ITEMNO) = TRIM(CAST(IPS.EAN AS Char(24)))
        WHERE IPS.WHS = 'ING' AND IPS.Acttype IN('OH','KA','KW')
    """,
    "INV_ADJ_OH_IPS": """
        SELECT IPS.Acttype AS REASONCODE, IPS.WHS, CAST(IPS.EAN AS Char(24)) AS EAN,
               I.[DESC] AS TITLE, IPS.Qty AS QTY
        FROM IPS.dbo.IPS_INV AS IPS
        LEFT JOIN TUTLIV.dbo.ICITEM I ON TRIM(I.ITEMNO) = TRIM(CAST(IPS.EAN AS Char(24)))
        WHERE IPS.WHS = 'IPS' AND IPS.Acttype IN('OH','KA','KW')
    """,
    "INV_ADJ_CC_ING": """
        SELECT IPS.Acttype AS REASONCODE, IPS.WHS, CAST(IPS.EAN AS Char(24)) AS EAN,
               I.[DESC] AS TITLE, IPS.Qty AS QTY
        FROM IPS.dbo.IPS_INV AS IPS
        LEFT JOIN TUTLIV.dbo.ICITEM I ON TRIM(I.ITEMNO) = TRIM(CAST(IPS.EAN AS Char(24)))
        WHERE IPS.WHS = 'ING' AND IPS.Acttype = 'CC'
    """,
    "INV_RR": """
        SELECT IPS.Acttype AS REASONCODE, IPS.WHS, CAST(IPS.EAN AS Char(24)) AS ISBN,
               I.[DESC] AS TITLE, IPS.Qty AS QTY
        FROM IPS.dbo.IPS_INV AS IPS
        LEFT JOIN TUTLIV.dbo.ICITEM I ON TRIM(I.ITEMNO) = TRIM(CAST(IPS.EAN AS Char(24)))
        WHERE IPS.Acttype = 'RR'
    """,
    "ADJ_S_R": """
        SELECT CAST(ISBN AS Char(24)) AS ISBN, ttl.TITLE, Ordnum, Otype, Ponumber,
               Otypesra, Billto, Billtoname, Qty AS QTY, Price, ROUND(Ext, 2) AS Ext, Discount
        FROM IPS.dbo.ips_daily_pre_ips_queries itm
        JOIN (SELECT ITEMNO, [DESC] TITLE FROM TUTLIV.dbo.ICITEM) ttl ON itm.ISBN = ttl.ITEMNO
        WHERE Substring(Otypesra,1,1) IN ('S', 'R')
    """,
    "INV_TI": """
        SELECT IPS.WHS, CAST(IPS.EAN AS Char(24)) AS EAN, I.[DESC] AS TITLE,
               IPS.Qty AS QTY, IPS.Acttype AS ACTTYPE
        FROM IPS.dbo.IPS_INV AS IPS
        LEFT JOIN TUTLIV.dbo.ICITEM I ON TRIM(I.ITEMNO) = TRIM(CAST(IPS.EAN AS Char(24)))
        WHERE IPS.Acttype = 'TI'
    """,
}

STANDARD_COLS = ["REASONCODE", "WHS", "EAN", "TITLE", "QTY"]


def generate_excel_inventory_adjustments():
    reports_dir = DailyFilesContext.daily_files_path().joinpath("Reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    with get_db() as engine:
        with engine.begin() as conn:
            res = conn.execute(
                text(
                    "SELECT TRIM(CAST(ReportName AS VARCHAR(50))) FROM IPS.dbo.Reports WHERE Data = 'X'"
                )
            )
            available_reports = [row[0] for row in res if row[0]]

        if not available_reports:
            raise RuntimeError(
                "No available reports with data found in IPS.dbo.Reports for the legacy daily files process."
            )

        for report_name in available_reports:
            if report_name not in REPORT_SQL:
                logging.warning("No SQL for report %s, skipping.", report_name)
                continue

            df = pd.read_sql_query(text(REPORT_SQL[report_name]), con=engine)
            if df.empty:
                raise RuntimeError(
                    f"Report {report_name} marked as having data but query returned no rows."
                )

            has_standard = all(c in df.columns for c in STANDARD_COLS)
            if not has_standard and "ISBN" in df.columns:
                order = ["REASONCODE", "WHS", "ISBN", "TITLE", "QTY"]
                has_standard = all(c in df.columns for c in order)
                if has_standard:
                    df = df[order]
            elif has_standard:
                df = df[STANDARD_COLS]

            startrow = 0 if report_name in CC_REPORTS else 1
            path = reports_dir.joinpath(f"{report_name}.xlsx")
            with pd.ExcelWriter(path) as writer:
                df.to_excel(writer, sheet_name=report_name, index=False, startrow=startrow)
            logging.info("Wrote %s to %s", report_name, path)


if __name__ == "__main__":
    generate_excel_inventory_adjustments()
