import os
import glob
import logging
import sys
from logic.manual_rerun_logic import proccess_daily_files_rerun

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    rerun_resources = r"\\tutpub3\VOL2\FOXPRO\TestFiles\Manual_Reruns\rerun_resources"
    rerun_output    = r"\\tutpub3\VOL2\FOXPRO\TestFiles\Manual_Reruns\rerun_output"

    cdt_files   = glob.glob(os.path.join(rerun_resources, "*.CDT"))
    cdp_files   = glob.glob(os.path.join(rerun_resources, "*.CDP"))
    trans_files = (
        glob.glob(os.path.join(rerun_resources, "*.TXT")) +
        glob.glob(os.path.join(rerun_resources, "*.txt"))
    )

    missing = []
    if not cdt_files:   missing.append(".CDT file")
    if not cdp_files:   missing.append(".CDP file")
    if not trans_files: missing.append("TransactionFile .TXT")

    if missing:
        logging.error(f"Missing required files in {rerun_resources}: {', '.join(missing)}")
        sys.exit(1)

    logging.info(f"CDT:   {cdt_files[0]}")
    logging.info(f"CDP:   {cdp_files[0]}")
    logging.info(f"Trans: {trans_files[0]}")
    logging.info(f"Output: {rerun_output}")
    logging.info("Outputs: Transfer.csv, ING_Transfers.csv, LOCKEDT.TXT, INPRO.TXT, "
                 "TRANSFER_SAGE_UPLOAD.xlsx, RV_SAGE_UPLOAD.xlsx, CR_SAGE_UPLOAD.xlsx, SL_SAGE_UPLOAD.xlsx")

    proccess_daily_files_rerun(cdt_files[0], cdp_files[0], trans_files[0], rerun_output)
