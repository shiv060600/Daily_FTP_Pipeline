import logging
from pathlib import Path
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib import colors
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


def _write_pdf(df: pd.DataFrame, path: Path, title: str = "") -> None:
    doc = SimpleDocTemplate(str(path), pagesize=landscape(A4))
    styles = getSampleStyleSheet()
    flowables = []

    if title:
        flowables.append(Paragraph(title, styles["Title"]))
        flowables.append(Spacer(1, 12))

    data = [df.columns.tolist()] + [
        [str(v) if v is not None else "" for v in row]
        for row in df.itertuples(index=False)
    ]
    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND",      (0, 0), (-1, 0),  colors.HexColor("#f2f2f2")),
        ("FONTNAME",        (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",        (0, 0), (-1, -1), 9),
        ("GRID",            (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("ROWBACKGROUNDS",  (0, 1), (-1, -1), [colors.white, colors.HexColor("#fafafa")]),
        ("ALIGN",           (0, 0), (-1, -1), "LEFT"),
        ("VALIGN",          (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",     (0, 0), (-1, -1), 8),
        ("RIGHTPADDING",    (0, 0), (-1, -1), 8),
        ("TOPPADDING",      (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",   (0, 0), (-1, -1), 5),
    ]))
    flowables.append(table)
    doc.build(flowables)

def generate_daily_reports(path : str | None = None):

    if path is not None:
        reports_dir = Path(path).joinpath("Test_Reports")
        new_pdf_reports_path = Path(path).joinpath("Test_Reports")
    else:
        reports_dir = DailyFilesContext.daily_files_path().joinpath("Reports")
        new_pdf_reports_path = DailyFilesContext.daily_files_path().joinpath("New_Reports")
    
    #ensure directories exist
    reports_dir.mkdir(parents=True, exist_ok=True)
    new_pdf_reports_path.mkdir(parents=True, exist_ok=True)

    #no truncating long text
    pd.set_option('display.max_colwidth', None)

    with get_db() as db:
        with db.begin() as conn:
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

        logging.info("beginning excel write to reports path")
        for report in available_reports:
            if report not in REPORT_SQL:
                logging.warning("report not in report sql skipping")
                continue
            df: pd.DataFrame = pd.read_sql_query(text(REPORT_SQL[report]), con=db)

            if df.empty:
                raise RuntimeError(
                    f"Report {report} marked as having data but query returned no rows."
                )

            has_standard = all(c in df.columns for c in STANDARD_COLS)

            if not has_standard and "ISBN" in df.columns:
                order = ["REASONCODE", "WHS", "ISBN", "TITLE", "QTY"]
                has_standard = all(c in df.columns for c in order)
                if has_standard:
                    df = df[order]

            elif has_standard:
                df = df[STANDARD_COLS]

            startrow = 0 if report in CC_REPORTS else 1
            path = reports_dir.joinpath(f"{report}.xlsx")

            with pd.ExcelWriter(path) as writer:
                df.to_excel(writer, sheet_name=report, index=False, startrow=startrow)

            logging.info("Wrote %s to %s (excel version)", report, path)

            _write_pdf(df, new_pdf_reports_path.joinpath(f"{report}.pdf"), title=report)

    return "Passed"



if __name__ == "__main__":
    generate_daily_reports()
