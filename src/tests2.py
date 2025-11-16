from datetime import datetime

import numpy as np

from src.heuristics.human_expert_v3 import HumanExpert
from src.db_handler.xl_database import ExcelSQL
from src.utils.constants import MINUS_TIME, WITHIN, BETWEEN
from src.utils.utils import get_now
import pandas as pd
import csv

RULE_1_to_9_account = "../sensitive_data/tests/acount and passwords.xlsx"
MESSAGES = "../sensitive_data/tests/messages2.xlsx"
APP_DATA = "../sensitive_data/tests/app_data.xlsx"

MESSAGES_LIMIT = 30

rule_conversion_dict = pd.read_csv("../sensitive_data/tests/rule_conversion.csv").set_index('Rule number')['new rule number'].to_dict()
rule_conversion_dict2 = {}
for k, v in rule_conversion_dict.items():
    try:
        if np.isnan(k):
            continue
    except:
        pass
    rule_conversion_dict2[str(k).lower()] = int(v)
rule_conversion_dict = rule_conversion_dict2


TREND_NUMBER_TO_NAME = {
    0: "INC",
    1: "STAG",
    2: "DET",
    3: "MISC",
}


# --- Configuration -----------------------------------------------------------

REC_IDX = 11  # the index you're consistently using
ALLOWED_SUFFIXES = ("1", "2", "3")  # adjust if needed


TEST_CASES = [
    # rule, date_str (DD.MM.YYYY), file

    # ("2", "24.06.2025", "../sensitive_data/tests/DB QA - rule 1 24.6- Female Morning FP(2).xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None, None),
    # ("1", "24.06.2025", "../sensitive_data/tests/DB QA - rule 1 24.6- Female Morning stag within.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "STAG", WITHIN),
    # ("1", "25.06.2025", "../sensitive_data/tests/DB QA - rule 1 - improve within 25.6.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC", WITHIN),
    # ("10", "24.06.2025", "../sensitive_data/tests/DB QA - rule 10 24.6- Female Morning DETER within.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", "DET", WITHIN),
    # ("2", "25.06.2025", "../sensitive_data/tests/DB QA - rule 2 25.6 - Female Morning  improve NW.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC", BETWEEN),
    # ("-2", "25.06.2025", "../sensitive_data/tests/DB QA - rule 2 25.6 - Female Morning  stag within FP.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "STAG", WITHIN),
    # ("2", "25.06.2025", "../sensitive_data/tests/DB QA - rule 2 25.6- Male Evening improve NW.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC", BETWEEN),
    # ("2", "25.06.2025", "../sensitive_data/tests/DB QA - rule 2 25.6- Male Evening stag NW .xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "STAG", BETWEEN),
    # ("3", "26.06.2025", "../sensitive_data/tests/DB QA - rule 3 26.6- Female  Morning IMROV NW .xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC", BETWEEN),
    # ("4", "26.06.2025", "../sensitive_data/tests/DB QA - rule 3 26.6- Female  Morning STAG  FP(4).xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "STAG", None),
    # ("3", "26.06.2025", "../sensitive_data/tests/DB QA - rule 3 26.6- Male Evening IMPROVE WITHIN.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC", WITHIN),
    # ("3", "26.06.2025", "../sensitive_data/tests/DB QA - rule 3 26.6- Male Evening IMROV NW .xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC", BETWEEN),
    # ("4", "29.06.2025", "../sensitive_data/tests/DB QA - rule 3 29.6 - Female Morning STAG WITHIN FP(4).xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "STAG", WITHIN),
    # ("3b", "29.06.2025", "../sensitive_data/tests/DB QA - rule 3b 29.6 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None, None),
    # ("4", "01.07.2025", "../sensitive_data/tests/DB QA - rule 4 1.7- Female Morning.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None, None),
    # ("4", "01.07.2025", "../sensitive_data/tests/DB QA - rule 4 1.7- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None, None),
    # ("4", "29.06.2025", "../sensitive_data/tests/DB QA - rule 4 29.6 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None, None),
    # ("5", "29.06.2025", "../sensitive_data/tests/DB QA - rule 5 29.6 - Female Morning ALL DETER.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "DET", None),
    # ("5", "29.06.2025", "../sensitive_data/tests/DB QA - rule 5 29.6 - Male Evening ALL DETER.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "DET", None),
    # ("3", "29.06.2025", "../sensitive_data/tests/DB QA - rule 5 29.6 - Male Evening IMPROV FP(3).xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC", None),
    # ("6", "08.07.2025", "../sensitive_data/tests/DB QA - rule 6 8.7 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None, None),
    # ("6", "08.07.2025", "../sensitive_data/tests/DB QA - rule 6 8.7- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None, None),
    # ("7", "14.02.2025", "../sensitive_data/tests/DB QA - rule 7 14.2- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None, None),
    # ("7", "15.02.2025", "../sensitive_data/tests/DB QA - rule 7 15.2 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None, None),
    # ("8", "01.03.2025", "../sensitive_data/tests/DB QA - rule 8 1.3- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None, None),
    # ("8", "21.02.2025", "../sensitive_data/tests/DB QA - rule 8 21.2 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None, None),
    # ("9", "23.03.2025", "../sensitive_data/tests/DB QA - rule 9 23.3 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None, None),
    # ("9", "24.03.2025", "../sensitive_data/tests/DB QA - rule 9 24.3- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None, None),
    # ("12", "19.01.2025", "../sensitive_data/tests/DB QA - rule 12 19.01 - Female Morning IMPRO.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", "INC", None),
    # ("12", "19.01.2025", "../sensitive_data/tests/DB QA - rule 12 19.01 - Male Morning IMPRO.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", "INC", None),
    # ("12", "18.01.2025", "../sensitive_data/tests/DB QA - rule 12 - missing exc -  18.01 - Female Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("12", "18.01.2025", "../sensitive_data/tests/DB QA - rule 12 - missing exc -  18.01 - Male Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("-12", "18.01.2025", "../sensitive_data/tests/DB QA - rule 12 false - all completed 18.01 - Female Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("13", "18.01.2025", "../sensitive_data/tests/DB QA - rule 13 - missing exc  - 18.01 - Female Morning  ALL DETER.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("13", "18.01.2025", "../sensitive_data/tests/DB QA - rule 13 - missing exc  - 18.01 - Male  Morning  ALL DETER.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("14", "26.01.2025", "../sensitive_data/tests/DB QA - rule 14 - missing exc -  26.01 - Female Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("14", "26.01.2025", "../sensitive_data/tests/DB QA - rule 14 - missing exc -  26.01 - Male Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("15", "26.01.2025", "../sensitive_data/tests/DB QA - rule 15 - missing exc  - 26.01 - Female Morning  ALL DETER.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("15", "26.01.2025", "../sensitive_data/tests/DB QA - rule 15 - missing exc  - 26.01 - Male Morning  ALL DETER.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    #
    # ("12", "18.01.2025", "../sensitive_data/tests/DB QA - rule 12 - didnt start next unit -  18.01 - Female Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("12", "18.01.2025", "../sensitive_data/tests/DB QA - rule 12 - didnt start next unit -  18.01 - Female Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("12", "18.01.2025", "../sensitive_data/tests/DB QA - rule 12 - didnt start next unit -  18.01 - Male  Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("12", "18.01.2025", "../sensitive_data/tests/DB QA - rule 12 - didnt start next unit -  18.01 - Male  Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("13", "18.01.2025", "../sensitive_data/tests/DB QA - rule 13 - didnt start next unit -  18.01 - Malle Morning ALL DETE.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("16", "25.01.2025", "../sensitive_data/tests/DB QA - rule 16 - missing exc -  25.01 - Male Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("16", "25.01.2025", "../sensitive_data/tests/DB QA - rule 16 - missing exc -  25.01 - Male Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("17", "25.01.2025", "../sensitive_data/tests/DB QA - rule 17 - missing exc -  25.01 - Male Morning ALL DETER.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("12", "20.01.2025", "../sensitive_data/tests/DB QA - rule 12 - missing exc 2nd time message -  20.01 - Male Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    #
    # ("18", "07.02.2025", "../sensitive_data/tests/DB QA - rule 18 - missing exc -  07.02 - Female Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("18", "07.02.2025", "../sensitive_data/tests/DB QA - rule 18 - missing exc -  07.02 - Female Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("18", "07.02.2025", "../sensitive_data/tests/DB QA - rule 18 - missing exc -  07.02 - Male Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("18", "07.02.2025", "../sensitive_data/tests/DB QA - rule 18 - missing exc -  07.02 - Male Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("19", "07.02.2025", "../sensitive_data/tests/DB QA - rule 19 - missing exc -  07.02 - Female Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("19", "07.02.2025", "../sensitive_data/tests/DB QA - rule 19 - missing exc -  07.02 - Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("20", "07.02.2025", "../sensitive_data/tests/DB QA - rule 20 - missing exc -  07.02 - Female Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("20", "07.02.2025", "../sensitive_data/tests/DB QA - rule 20 - missing exc -  07.02 - Female Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("20", "07.02.2025", "../sensitive_data/tests/DB QA - rule 20 - missing exc -  07.02 - Male Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("20", "07.02.2025", "../sensitive_data/tests/DB QA - rule 20 - missing exc -  07.02 - Male Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("21", "07.02.2025", "../sensitive_data/tests/DB QA - rule 21 - missing exc -  07.02 - Female Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("21", "07.02.2025", "../sensitive_data/tests/DB QA - rule 21 - missing exc -  07.02 - Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("22", "07.02.2025", "../sensitive_data/tests/DB QA - rule 22 - missing exc -  07.02 - Female Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("22", "07.02.2025", "../sensitive_data/tests/DB QA - rule 22 - missing exc -  07.02 - Female Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("22", "07.02.2025", "../sensitive_data/tests/DB QA - rule 22 - missing exc -  07.02 - Male Morning IMPRO.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "INC", None),
    # ("22", "07.02.2025", "../sensitive_data/tests/DB QA - rule 22 - missing exc -  07.02 - Male Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("23", "07.02.2025", "../sensitive_data/tests/DB QA - rule 23 - missing exc -  07.02 - Female Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("23", "07.02.2025", "../sensitive_data/tests/DB QA - rule 23 - missing exc -  07.02 - Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("26", "18.01.2025", "../sensitive_data/tests/DB QA - rule 26 - missing exc -  18.01 - Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("26", "03.03.2025", "../sensitive_data/tests/DB QA - rule 26 - future -  03.03 - Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("26", "18.01.2025", "../sensitive_data/tests/DB QA - rule 26 - missing exc -  18.01 - Female Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),

    # ("15", "23.01.2025", "../sensitive_data/tests/DB QA - rule 66 but rule 15 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # TODO add a test for messages limit

    # ("24", "18.01.2025", "../sensitive_data/tests/DB QA - rule 24 - unit 1 -  18.01 - Male Morning.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("-24", "18.01.2025", "../sensitive_data/tests/DB QA - rule -24 - unit -2 but not enough from units-  18.01 - Male Morning.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("24", "18.01.2025", "../sensitive_data/tests/DB QA - rule 24 - unit 2 -  18.01 - Male Morning.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("24", "18.01.2025", "../sensitive_data/tests/DB QA - rule 24 - unit 3 -  18.01 - Male Morning.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("25", "18.01.2025", "../sensitive_data/tests/DB QA - rule 24 - unit 4 -  18.01 - Male Morning.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),




    # ("28", "10.02.2025", "../sensitive_data/tests/DB QA - rule 28 -  18.01 - Male Morning.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("29", "10.04.2025", "../sensitive_data/tests/DB QA - rule 28 -  18.01 - Male Morning.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("30", "10.05.2025", "../sensitive_data/tests/DB QA - rule 28 -  18.01 - Male Morning.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("31", "08.02.2025", "../sensitive_data/tests/DB QA - rule 29 -  18.01 - Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("31", "08.02.2025", "../sensitive_data/tests/DB QA - rule 29 -  18.01 - Male Morning STAG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("32", "08.02.2025", "../sensitive_data/tests/DB QA - rule 29 -  18.01 - Male Morning.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),

    # ("33", "12.02.2025", "../sensitive_data/tests/DB QA - rule 33 -  18.01 - Male Morning STAG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("34", "13.02.2025", "../sensitive_data/tests/DB QA - rule 34 -  18.01 - Male Morning STAG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("35", "27.01.2025", "../sensitive_data/tests/DB QA - rule 35 -  18.01 - Male Morning STAG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("36", "13.02.2025", "../sensitive_data/tests/DB QA - rule 36 -  18.01 - Male Morning STAG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("37", "13.02.2025", "../sensitive_data/tests/DB QA - rule 37 -  18.01 - Male Morning STAG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    #
    # ("38", "08.02.2025", "../sensitive_data/tests/DB QA - rule 38 -  18.01 -  3-1 Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("38", "08.02.2025", "../sensitive_data/tests/DB QA - rule 38 -  18.01 -  3-2 Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("39", "08.02.2025", "../sensitive_data/tests/DB QA - rule 39 -  18.01 -  2-1 Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    #
    # ("40", "08.02.2025", "../sensitive_data/tests/DB QA - rule 40 -  18.01 -  2-1 Male Morning DET.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("41", "08.02.2025", "../sensitive_data/tests/DB QA - rule 41 -  18.01 -  2-1 Male Morning DET.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("42", "08.02.2025", "../sensitive_data/tests/DB QA - rule 42 -  18.01 -  2-1 Male Morning DET.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("43", "08.02.2025", "../sensitive_data/tests/DB QA - rule 43 -  18.01 -  2-3 Male Morning DET.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("43", "08.02.2025", "../sensitive_data/tests/DB QA - rule 43 -  18.01 -  1-3 Male Morning DET.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("44", "08.02.2025", "../sensitive_data/tests/DB QA - rule 44 -  18.01 -  2-1 Male Morning DET.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),

    # ("45", "01.02.2025", "../sensitive_data/tests/DB QA - rule 44 -  18.01 -  2-1 Male Morning STEG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("46", "05.02.2025", "../sensitive_data/tests/DB QA - rule 44 -  18.01 -  2-1 Male Morning STEG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("47", "13.02.2025", "../sensitive_data/tests/DB QA - rule 47 -  18.01 -  2-1 Male Morning STEG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("48", "19.03.2025", "../sensitive_data/tests/DB QA - rule 44 -  18.01 -  2-1 Male Morning STEG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("49", "26.04.2025", "../sensitive_data/tests/DB QA - rule 44 -  18.01 -  2-1 Male Morning STEG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),

    # ("50", "12.01.2025", "../sensitive_data/tests/DB QA - rule 50 -  18.01 -  2-1 Male Morning STEG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("51", "12.01.2025", "../sensitive_data/tests/DB QA - rule 51 -  18.01 -  2-1 Male Morning STEG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("51", "12.01.2025", "../sensitive_data/tests/DB QA - rule 51 -  18.01 - 3-3 Male Morning STEG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("52", "12.01.2025", "../sensitive_data/tests/DB QA - rule 52 -  18.01 - Male Morning STEG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("53", "12.01.2025", "../sensitive_data/tests/DB QA - rule 53 -  18.01 - Male Morning STEG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("53", "12.01.2025", "../sensitive_data/tests/DB QA - rule 53 -  18.01 - 3-3 -Male Morning STEG.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "STAG", None),
    # ("54", "12.01.2025", "../sensitive_data/tests/DB QA - rule 54 -  18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("55", "12.01.2025", "../sensitive_data/tests/DB QA - rule 55 -  18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("55", "12.01.2025", "../sensitive_data/tests/DB QA - rule 55 - 3-3 18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("56", "12.01.2025", "../sensitive_data/tests/DB QA - rule 56 - 3-1 18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("56", "12.01.2025", "../sensitive_data/tests/DB QA - rule 56 - 2-1 18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("57", "12.01.2025", "../sensitive_data/tests/DB QA - rule 57 - 3-2 18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("58", "12.01.2025", "../sensitive_data/tests/DB QA - rule 58 -  18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("59", "12.01.2025", "../sensitive_data/tests/DB QA - rule 59 -  18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("59", "12.01.2025", "../sensitive_data/tests/DB QA - rule 59 - 3-3 18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("60", "12.01.2025", "../sensitive_data/tests/DB QA - rule 60 - 3-1 18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("60", "12.01.2025", "../sensitive_data/tests/DB QA - rule 60 - 2-1 18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("61", "12.01.2025", "../sensitive_data/tests/DB QA - rule 61 - 3-2 18.01 -Male Morning IMP.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "INC", None),
    # ("62", "12.01.2025", "../sensitive_data/tests/DB QA - rule 62 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("63", "12.01.2025", "../sensitive_data/tests/DB QA - rule 63 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("64", "12.01.2025", "../sensitive_data/tests/DB QA - rule 64 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("65", "12.01.2025", "../sensitive_data/tests/DB QA - rule 65 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("66", "17.01.2025", "../sensitive_data/tests/DB QA - rule 66 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("67", "20.01.2025", "../sensitive_data/tests/DB QA - rule 67 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("68", "17.01.2025", "../sensitive_data/tests/DB QA - rule 68 -  18.01 -Male Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("69", "18.01.2025", "../sensitive_data/tests/DB QA - rule 69 -  18.01 -Male Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("70", "18.01.2025", "../sensitive_data/tests/DB QA - rule 70 -  18.01 -Male Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("71", "19.01.2025", "../sensitive_data/tests/DB QA - rule 71 -  18.01 -Male Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("72", "17.01.2025", "../sensitive_data/tests/DB QA - rule 72 -  18.01 -Male Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("73", "19.01.2025", "../sensitive_data/tests/DB QA - rule 73 -  18.01 -Male Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),
    # ("74", "29.01.2025", "../sensitive_data/tests/DB QA - rule 74 -  18.01 -Male Morning STAG.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "STAG", None),

    # ("75", "12.01.2025", "../sensitive_data/tests/DB QA - rule 75 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),

    # ("76", "12.01.2025", "../sensitive_data/tests/DB QA - rule 76 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("-76", "12.01.2025", "../sensitive_data/tests/DB QA MESSAGE 2 DAYS - rule 76 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    # ("-76", "12.01.2025", "../sensitive_data/tests/DB QA MESSAGE SAME - rule 76 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),
    ("76", "13.01.2025", "../sensitive_data/tests/DB QA MESSAGE SAME LATER - rule 76 -  18.01 -Male Morning DET.xlsx", "../sensitive_data/tests/account and passwords3.xlsx", "DET", None),



    # ("0", "09.11.2025", "../sensitive_data/tests/DB QA - carousel 1 test.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("0", "09.11.2025", "../sensitive_data/tests/DB QA - carousel 1 test cs2-cs1.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("0", "09.11.2025", "../sensitive_data/tests/DB QA - carousel 1 test cs2-cs1 evening.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),
    # ("0", "09.11.2025", "../sensitive_data/tests/DB QA - carousel 1 test 3 evening.xlsx", "../sensitive_data/tests/acount and passwords_T2.xlsx", "DET", None),




]
# --- Helpers -----------------------------------------------------------------

def _expected_rule_value(rule: str):
    """
    Map the string rule key to the expected value used in recommendations[REC_IDX]["message"][1][0].
    Keeps support for alphanumeric rules like '3a'/'3b'.
    """
    return int(rule) if rule.isdigit() else rule_conversion_dict.get(rule, rule)

    if rule.isdigit():
        return rule_conversion_dict.get(rule, int(rule))
    # non-digit rules: don't force int() fallback
    return rule_conversion_dict.get(rule, rule)

def _sex_from_filename(path: str):
    low = path.lower()
    if " female " in low:
        return "f"
    if " male " in low:
        return "m"
    return None

def _safe_rec(recommendations, idx=REC_IDX):
    if not recommendations:
        raise AssertionError("Recommendations should not be empty")
    if 0 >= len(recommendations):
        raise IndexError(f"Expected at least {idx+1} recommendations, got {len(recommendations)}")
    return recommendations[idx]

def _check_rule_id(rule: str, rec_msg):
    # rec_msg expected structure: ["message", [...], ...]; you used [1][0]
    expected, negative_number = name_to_number(rule)
    if expected == 0:
        print("\t\t\tskipping rule check because it is 0")
        return

    try:
        if negative_number and len(rec_msg[1]) == 0: # should not be a specific rule, and got no rule - that is ok
            return
        rec_rule = rec_msg[1][0]
    except Exception as e:
        print("\t\t\t[WARN] Unexpected message shape; cannot read rule id:", e)
        return
    # NOTE: your original code prints "rule not found" when they ARE equal.
    # That seems inverted. Usually we'd warn when they are NOT equal.
    if not negative_number and rec_rule != expected:
        print("\t\t\t[WARN] rule mismatch:", rec_rule, "!= expected", expected)
    if negative_number and rec_rule == expected:
        print("\t\t\t[WARN] rule should not match:", rec_rule, "== expected", expected)


def name_to_number(rule):
    negative_number = rule[0] == "-"
    if negative_number:
        rule = rule[1:]
    expected = _expected_rule_value(rule)
    return expected, negative_number


def _check_sex_and_message_codes(rule: str, file: str, rec_msg):
    sex = _sex_from_filename(file)
    if sex is None:
        return  # no sex encoded in filename -> nothing to validate

    try:
        rec_sex_code = rec_msg[2].lower()
    except Exception as e:
        print("\t\t\t[WARN] Unexpected message shape; cannot read sex code:", e)
        return

    if rec_sex_code not in ("f", "m"):
        print("\t\t\t[WARN] sex code should be 'f' or 'm' but got", rec_sex_code)

    if rec_sex_code != sex:
        print(f"\t\t\t[WARN] sex should be {sex} but got {rec_sex_code}")

    allowed = [f"{rule}-f{s}" for s in ALLOWED_SUFFIXES] if sex == "f" else \
              [f"{rule}-m{s}" for s in ALLOWED_SUFFIXES]

    if all(rec_sex_code not in a or rule not in a for a in allowed ) and rec_sex_code in ("f", "m"):
        # rec_sex_code is just 'f'/'m' while your old checks expected 'rule-f1' etc.
        # If the design is that element [2] should be the detailed code, leave this warn.
        print("\t\t\t[WARN] message code not as expected. got:", rec_sex_code, " expected one of:", allowed)

def _check_trend(trend_to_be, trend_num):
    """
    Extracts the numeric trend from rec_msg["trend"],
    converts it to a string via TREND_NUMBER_TO_NAME,
    and prints a warning if conversion fails.
    """
    if trend_to_be is None:
        return  # no trend to check, skip

    if trend_num is None:
        print("\t\t\t[WARN] No 'trend' field found in message:", trend_num)
        return

    trend_name = TREND_NUMBER_TO_NAME.get(trend_num)
    if trend_name is None:
        print(f"\t\t\t[WARN] Unknown trend number {trend_num} (not in TREND_NUMBER_TO_NAME)")
        return

    if trend_name != trend_to_be:
        print(f"\t\t\t[WARN] Expected trend {trend_to_be}, but got {trend_name} (from number {trend_num})")
        return

def _check_within_or_between(within_or_between: int, response: int):
    if within_or_between is None:
        return
    if within_or_between not in (0, 1):
        raise ValueError(f"Invalid value for within_or_between: {within_or_between}. Expected 0 or 1.")
    if within_or_between == 1 and response == 0:
        print("\t\t\t[WARN] Expected 'WITHIN' (0 in code, 1 in json) but got 'BETWEEN' (1 in code, 0 in json) in response.")
    if within_or_between == 0 and response == 1:
        print("\t\t\t[WARN] Expected 'BETWEEN' (1 in code, 0 in json) in response but got 'WITHIN' (0 in code, 1 in json).")

# --- Core runner -------------------------------------------------------------

def run_case(rule: str, date_str: str, file: str, T_file: str, trend: str, W_or_B: int):
    print(f"\t\t[{rule}] {file}")
    exec_sql = ExcelSQL([file, APP_DATA, T_file, MESSAGES])
    exec_sql.create_table()

    minus_days = (get_now(0).date() - datetime.strptime(date_str, "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    expected, negative_number = name_to_number(rule)
    recommendations = he.recommend(DEBUG=True, message_limit= expected+5 )

    rec = _safe_rec(recommendations, REC_IDX)
    msg = rec.get("message") if isinstance(rec, dict) else rec

    _check_rule_id(rule, msg)
    _check_sex_and_message_codes(rule, file, msg)
    recieved_trend = rec.get("trend") if isinstance(rec, dict) else rec
    _check_trend(trend, recieved_trend)
    _check_within_or_between(W_or_B, rec.get(WITHIN, None))

    print("\t\t\t", recommendations)

def run_all_cases():
    for rule, date_str, file, T_file, trend, W_or_B in TEST_CASES:
        try:
            run_case(rule, date_str, file, T_file, trend, W_or_B)
        except Exception as e:
            print(f"\t\t\t[ERROR] {rule} | {file} -> {e}")


run_all_cases()
