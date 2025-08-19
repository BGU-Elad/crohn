from datetime import datetime

import numpy as np

from src.heuristics.human_expert_v3 import HumanExpert
from src.db_handler.xl_database import ExcelSQL
from src.utils.constants import MINUS_TIME
from src.utils.utils import get_now
import pandas as pd
import csv

RULE_1_to_9_account = "../sensitive_data/tests/acount and passwords.xlsx"
MESSAGES = "../sensitive_data/tests/messages2.xlsx"
APP_DATA = "../sensitive_data/tests/app_data.xlsx"

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
    ("1",  "24.07.2025", "../sensitive_data/tests/DB QA - rule 1 male 24.7 evening .xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None),
    ("1",  "24.06.2025", "../sensitive_data/tests/DB QA - rule 1 24.6- Female Morning IMPRO.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC"),
    ("1",  "24.06.2025", "../sensitive_data/tests/DB QA - rule 1 24.6- Female Morning.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None),

    # ("10", "01.02.2025", "../sensitive_data/tests/DB QA - rule 10 1.2 - Male Evening DETER NWT.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "DET"),

    ("2",  "24.06.2025", "../sensitive_data/tests/DB QA - rule 2 24.6 - Female Morning  IMPROV.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC"),
    ("2",  "24.06.2025", "../sensitive_data/tests/DB QA - rule 2 24.6 - Female Morning  STAG.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "STAG"),
    ("2",  "24.06.2025", "../sensitive_data/tests/DB QA - rule 2 24.6- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None),

    ("3",  "25.06.2025", "../sensitive_data/tests/DB QA - rule 3 25.6- Female  Morning IMROV .xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC"),
    ("3",  "25.06.2025", "../sensitive_data/tests/DB QA - rule 3 25.6- Male Evening IMROV .xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC"),
    # ("3",  "26.06.2025", "../sensitive_data/tests/DB QA - rule 3 26.6- Female  Morning IMROV .xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC"),
    ("3",  "26.06.2025", "../sensitive_data/tests/DB QA - rule 3 26.6- Female  Morning STAG .xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "STAG"),
    # ("3",  "26.06.2025", "../sensitive_data/tests/DB QA - rule 3 26.6- Male Evening IMROV .xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC"),
    ("3",  "29.06.2025", "../sensitive_data/tests/DB QA - rule 3 29.6 - Female Morning STAG.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "STAG"),

    ("3a", "25.06.2025", "../sensitive_data/tests/DB QA - rule 3a 25.6- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None),
    ("3a", "29.06.2025", "../sensitive_data/tests/DB QA - rule 3a 29.6 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None),

    # ("3b", "29.06.2025", "../sensitive_data/tests/DB QA - rule 3b 29.6 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None),

    # ("4",  "01.07.2025", "../sensitive_data/tests/DB QA - rule 4 1.7- Female Morning.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None),
    # ("4",  "01.07.2025", "../sensitive_data/tests/DB QA - rule 4 1.7- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None),
    # ("4",  "29.06.2025", "../sensitive_data/tests/DB QA - rule 4 29.6 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None),

    # ("5",  "29.06.2025", "../sensitive_data/tests/DB QA - rule 5 29.6 - Female Morning ALL DETER.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "DET"),
    ("5",  "29.06.2025", "../sensitive_data/tests/DB QA - rule 5 29.6 - Female Morning ALL STAG.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "STAG"),
    # ("5",  "29.06.2025", "../sensitive_data/tests/DB QA - rule 5 29.6 - Male Evening ALL DETER.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "DET"),
    ("5",  "29.06.2025", "../sensitive_data/tests/DB QA - rule 5 29.6 - Male Evening IMPROV.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", "INC"),

    # ("6",  "08.07.2025", "../sensitive_data/tests/DB QA - rule 6 8.7 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None),
    # ("6",  "08.07.2025", "../sensitive_data/tests/DB QA - rule 6 8.7- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords.xlsx", None),
    #
    # ("7",  "14.02.2025", "../sensitive_data/tests/DB QA - rule 7 14.2- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None),
    # ("7",  "15.02.2025", "../sensitive_data/tests/DB QA - rule 7 15.2 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None),
    #
    # ("8",  "01.03.2025", "../sensitive_data/tests/DB QA - rule 8 1.3- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None),
    # ("8",  "21.02.2025", "../sensitive_data/tests/DB QA - rule 8 21.2 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None),

    ("9",  "23.03.2025", "../sensitive_data/tests/DB QA - rule 9 23.3 - Female Morning.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None),
    # ("9",  "24.03.2025", "../sensitive_data/tests/DB QA - rule 9 24.3- Male Evening.xlsx", "../sensitive_data/tests/acount and passwords2.xlsx", None),
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
    try:
        rec_rule = rec_msg[1][0]
    except Exception as e:
        print("\t\t\t[WARN] Unexpected message shape; cannot read rule id:", e)
        return
    expected = _expected_rule_value(rule)
    # NOTE: your original code prints "rule not found" when they ARE equal.
    # That seems inverted. Usually we'd warn when they are NOT equal.
    if rec_rule != expected:
        print("\t\t\t[WARN] rule mismatch:", rec_rule, "!= expected", expected)

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


# --- Core runner -------------------------------------------------------------

def run_case(rule: str, date_str: str, file: str, T_file: str, trend: str):
    print(f"\t\t[{rule}] {file}")
    exec_sql = ExcelSQL([file, APP_DATA, T_file, MESSAGES])
    exec_sql.create_table()

    minus_days = (get_now(0).date() - datetime.strptime(date_str, "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    rec = _safe_rec(recommendations, REC_IDX)
    msg = rec.get("message") if isinstance(rec, dict) else rec

    _check_rule_id(rule, msg)
    _check_sex_and_message_codes(rule, file, msg)
    recieved_trend = rec.get("trend") if isinstance(rec, dict) else rec
    _check_trend(trend, recieved_trend)

    print("\t\t\t", recommendations)

def run_all_cases():
    for rule, date_str, file, T_file, trend in reversed(TEST_CASES):
        try:
            run_case(rule, date_str, file, T_file, trend)
        except Exception as e:
            print(f"\t\t\t[ERROR] {rule} | {file} -> {e}")


run_all_cases()
