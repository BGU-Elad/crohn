from datetime import datetime

import numpy as np

from src.heuristics.human_expert_v3 import HumanExpert
from src.db_handler.xl_database import ExcelSQL
from src.utils.constants import MINUS_TIME
from src.utils.utils import get_now
import pandas as pd
import csv

RULE_1_to_9_account = "../sensitive_data/tests/acount and passwords.xlsx"
MESSAGES = "../sensitive_data/tests/messages.xlsx"
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
# rule_conversion_dict = {str(k).lower(): int(v) for k, v in rule_conversion_dict.items()}

TREND_NUMBER_TO_NAME = {
    0: "INC",
    1: "STAG",
    2: "DET",
    3: "MISC",
}


# print(rule_conversion)
#
# files = [
#     ("DB QA - rule 1 24.6- Female Morning IMPRO.xlsx", "24.06.2025"),
#     ("DB QA - rule 1 24.6- Female Morning.xlsx", "24.06.2025"),
#     ("DB QA - rule 10 1.2 - Male Evening DETER NWT.xlsx", "1.02.2025"),
#     ("DB QA - rule 2 24.6 - Female Morning  IMPROV.xlsx", "24.06.2025"),
#     ("DB QA - rule 2 24.6 - Female Morning  STAG.xlsx", "24.06.2025"),
#     ("DB QA - rule 2 24.6- Male Evening.xlsx", "24.06.2025"),
#     ("DB QA - rule 3 25.6- Female  Morning IMROV .xlsx", "25.06.2025"),
#     ("DB QA - rule 3 25.6- Male Evening IMROV .xlsx", "25.06.2025"),
#     ("DB QA - rule 3 26.6- Female  Morning IMROV .xlsx", "26.06.2025"),
#     ("DB QA - rule 3 26.6- Female  Morning STAG .xlsx", "26.06.2025"),
#     ("DB QA - rule 3 26.6- Male Evening IMROV .xlsx", "26.06.2025"),
#     ("DB QA - rule 3 29.6 - Female Morning STAG.xlsx", "29.06.2025"),
#     ("DB QA - rule 3a 25.6- Male Evening.xlsx", "25.06.2025"),
#     ("DB QA - rule 3a 29.6 - Female Morning.xlsx", "29.06.2025"),
#     ("DB QA - rule 3b 29.6 - Female Morning.xlsx", "29.06.2025"),
#     ("DB QA - rule 4 1.7- Female Morning.xlsx", "1.07.2025"),
#     ("DB QA - rule 4 1.7- Male Evening.xlsx", "1.07.2025"),
#     ("DB QA - rule 4 29.6 - Female Morning.xlsx", "29.06.2025"),
#     ("DB QA - rule 5 29.6 - Female Morning ALL DETER.xlsx", "29.06.2025"),
#     ("DB QA - rule 5 29.6 - Female Morning ALL STAG.xlsx", "29.06.2025"),
#     ("DB QA - rule 5 29.6 - Male Evening ALL DETER.xlsx", "29.06.2025"),
#     ("DB QA - rule 5 29.6 - Male Evening IMPROV.xlsx", "29.06.2025"),
#     ("DB QA - rule 6 8.7 - Female Morning.xlsx", "8.07.2025"),
#     ("DB QA - rule 6 8.7- Male Evening.xlsx", "8.07.2025"),
#     ("DB QA - rule 7 14.2- Male Evening.xlsx", "14.02.2025"),
#     ("DB QA - rule 7 15.2 - Female Morning.xlsx", "15.02.2025"),
#     ("DB QA - rule 8 1.3- Male Evening.xlsx", "1.03.2025"),
#     ("DB QA - rule 8 21.2 - Female Morning.xlsx", "21.02.2025"),
#     ("DB QA - rule 9 23.3 - Female Morning.xlsx", "23.03.2025"),
#     ("DB QA - rule 9 24.3- Male Evening.xlsx", "24.03.2025"),
# ]
#
# # for file, _ in files:
# #     d = file.split(" ")[5]
# #     if d[-1] == "-":
# #         d = d[:-1]
# #     print(d)
#
# for file, d in files:
#     no_ending = file.split(".")[-2]
#     no_beginning = no_ending.split("-")[1].strip().replace(" ", "_").lower()
#     splitted = file.split(" ")
#
#     func_name = "_".join(
#         [splitted[3], splitted[4], no_beginning]
#         )
#     # print(func_name+"()")
#     print(
#         f'''
#     def {func_name}():
#         exec_sql = ExcelSQL(["../sensitive_data/tests/{file}", APP_DATA, RULE_1_to_9_account, MESSAGES])
#         exec_sql.create_table()
#
#         minus_days = (get_now(0).date() - datetime.strptime("{d}", "%d.%m.%Y").date()).days
#         he = HumanExpert(exec_sql, minus_days)
#         recommendations = he.recommend(DEBUG=True)
#
#         assert len(recommendations) > 0, "Recommendations should not be empty"
#         if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
#         print("\t", "rule not found in recommendations")
#         print("\t\t\t",recommendations)
#
#         '''
#
#     )


def rule_1_male_evening():
    file = "../sensitive_data/tests/DB QA - rule 1 male 24.7 evening .xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "1"

    minus_days = (get_now(0).date() - datetime.strptime("24.07.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)

def rule_1_female_morning_impro():
    file = "../sensitive_data/tests/DB QA - rule 1 24.6- Female Morning IMPRO.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "1"

    minus_days = (get_now(0).date() - datetime.strptime("24.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_1_female_morning():
    file = "../sensitive_data/tests/DB QA - rule 1 24.6- Female Morning.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "1"

    minus_days = (get_now(0).date() - datetime.strptime("24.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_10_male_evening_deter_nwt():
    file = "../sensitive_data/tests/DB QA - rule 10 1.2 - Male Evening DETER NWT.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "10"

    minus_days = (get_now(0).date() - datetime.strptime("1.02.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_2_female_morning__improv():
    file = "../sensitive_data/tests/DB QA - rule 2 24.6 - Female Morning  IMPROV.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "2"

    minus_days = (get_now(0).date() - datetime.strptime("24.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_2_female_morning__stag():
    file = "../sensitive_data/tests/DB QA - rule 2 24.6 - Female Morning  STAG.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "2"

    minus_days = (get_now(0).date() - datetime.strptime("24.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_2_male_evening():
    file = "../sensitive_data/tests/DB QA - rule 2 24.6- Male Evening.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "2"

    minus_days = (get_now(0).date() - datetime.strptime("24.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_3_female__morning_imrov():
    file= "../sensitive_data/tests/DB QA - rule 3 25.6- Female  Morning IMROV .xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "3"

    minus_days = (get_now(0).date() - datetime.strptime("25.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_3_male_evening_imrov():
    file = "../sensitive_data/tests/DB QA - rule 3 25.6- Male Evening IMROV .xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "3"

    minus_days = (get_now(0).date() - datetime.strptime("25.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_3_female__morning_imrov():
    file = "../sensitive_data/tests/DB QA - rule 3 26.6- Female  Morning IMROV .xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "3"

    minus_days = (get_now(0).date() - datetime.strptime("26.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_3_female__morning_stag():
    file = "../sensitive_data/tests/DB QA - rule 3 26.6- Female  Morning STAG .xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()

    minus_days = (get_now(0).date() - datetime.strptime("26.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)
    rule = "3"

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_3_male_evening_imrov():
    file = "../sensitive_data/tests/DB QA - rule 3 26.6- Male Evening IMROV .xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "3"

    minus_days = (get_now(0).date() - datetime.strptime("26.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_3_female_morning_stag():
    file = "../sensitive_data/tests/DB QA - rule 3 29.6 - Female Morning STAG.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "3"

    minus_days = (get_now(0).date() - datetime.strptime("29.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_3a_male_evening():
    file = "../sensitive_data/tests/DB QA - rule 3a 25.6- Male Evening.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "3a"

    minus_days = (get_now(0).date() - datetime.strptime("25.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_3a_female_morning():
    file = "../sensitive_data/tests/DB QA - rule 3a 29.6 - Female Morning.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "3a"

    minus_days = (get_now(0).date() - datetime.strptime("29.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_3b_female_morning():
    file = "../sensitive_data/tests/DB QA - rule 3b 29.6 - Female Morning.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "3b"

    minus_days = (get_now(0).date() - datetime.strptime("29.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_4_female_morning():
    file = "../sensitive_data/tests/DB QA - rule 4 1.7- Female Morning.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "4"

    minus_days = (get_now(0).date() - datetime.strptime("1.07.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_4_male_evening():
    file = "../sensitive_data/tests/DB QA - rule 4 1.7- Male Evening.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "4"

    minus_days = (get_now(0).date() - datetime.strptime("1.07.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_4_female_morning():
    file = "../sensitive_data/tests/DB QA - rule 4 29.6 - Female Morning.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "4"

    minus_days = (get_now(0).date() - datetime.strptime("29.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_5_female_morning_all_deter():
    file = "../sensitive_data/tests/DB QA - rule 5 29.6 - Female Morning ALL DETER.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "5"

    minus_days = (get_now(0).date() - datetime.strptime("29.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_5_female_morning_all_stag():
    file = "../sensitive_data/tests/DB QA - rule 5 29.6 - Female Morning ALL STAG.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "5"

    minus_days = (get_now(0).date() - datetime.strptime("29.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_5_male_evening_all_deter():
    file = "../sensitive_data/tests/DB QA - rule 5 29.6 - Male Evening ALL DETER.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "5"

    minus_days = (get_now(0).date() - datetime.strptime("29.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_5_male_evening_improv():
    file = "../sensitive_data/tests/DB QA - rule 5 29.6 - Male Evening IMPROV.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account,
         MESSAGES])
    exec_sql.create_table()
    rule = "5"

    minus_days = (get_now(0).date() - datetime.strptime("29.06.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_6_female_morning():
    file = "../sensitive_data/tests/DB QA - rule 6 8.7 - Female Morning.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "6"

    minus_days = (get_now(0).date() - datetime.strptime("8.07.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)

def rule_6_male_evening():
    file = "../sensitive_data/tests/DB QA - rule 6 8.7- Male Evening.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "6"

    minus_days = (get_now(0).date() - datetime.strptime("8.07.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_7_male_evening():
    file = "../sensitive_data/tests/DB QA - rule 7 14.2- Male Evening.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "7"

    minus_days = (get_now(0).date() - datetime.strptime("14.02.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_7_female_morning():
    file = "../sensitive_data/tests/DB QA - rule 7 15.2 - Female Morning.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "7"

    minus_days = (get_now(0).date() - datetime.strptime("15.02.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_8_male_evening():
    file = "../sensitive_data/tests/DB QA - rule 8 1.3- Male Evening.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "8"

    minus_days = (get_now(0).date() - datetime.strptime("1.03.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_8_female_morning():
    file = "../sensitive_data/tests/DB QA - rule 8 21.2 - Female Morning.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "8"

    minus_days = (get_now(0).date() - datetime.strptime("21.02.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_9_female_morning():
    file = "../sensitive_data/tests/DB QA - rule 9 23.3 - Female Morning.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "9"

    minus_days = (get_now(0).date() - datetime.strptime("23.03.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


def rule_9_male_evening():
    file = "../sensitive_data/tests/DB QA - rule 9 24.3- Male Evening.xlsx"
    exec_sql = ExcelSQL(
        [file, APP_DATA, RULE_1_to_9_account, MESSAGES])
    exec_sql.create_table()
    rule = "9"

    minus_days = (get_now(0).date() - datetime.strptime("24.03.2025", "%d.%m.%Y").date()).days
    he = HumanExpert(exec_sql, minus_days)
    recommendations = he.recommend(DEBUG=True)

    assert len(recommendations) > 0, "Recommendations should not be empty"
    if rule_conversion_dict.get(rule, int(rule)) == recommendations[11]["message"][1][0]:
        print("\t\t\t", "rule not found in recommendations")
    if " female " in file.lower():
        if recommendations[11]["message"][2].lower() != "f":
            print("\t\t\tsex should be female but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-f1", f"{rule}-f2", f"{rule}-f3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])
    if " male " in file.lower():
        if recommendations[11]["message"][2].lower() != "m":
            print("\t\t\ttime should be male but got", recommendations[11]["message"][2])
        if recommendations[11]["message"][2].lower() not in [f"{rule}-m1", f"{rule}-m2", f"{rule}-m3"]:
            print("\t\t\tmessage was not as expect. got: ", recommendations[11]["message"][2])

    print("\t\t\t",recommendations)


# print("rule_1_male_evening")
# rule_1_male_evening()
# print("rule_1_female_morning_impro")
# rule_1_female_morning_impro()
# print("rule_1_female_morning")
# rule_1_female_morning()
# print("rule_10_male_evening_deter_nwt")
# rule_10_male_evening_deter_nwt()
# print("rule_2_female_morning__improv")
# rule_2_female_morning__improv()
# print("rule_2_female_morning__stag")
# rule_2_female_morning__stag()
# print("rule_2_male_evening")
# rule_2_male_evening()
# print("rule_3_female__morning_imrov")
# rule_3_female__morning_imrov()
# print("rule_3_male_evening_imrov")
# rule_3_male_evening_imrov()
# print("rule_3_female__morning_imrov")
# rule_3_female__morning_imrov()
# print("rule_3_female__morning_stag")
# rule_3_female__morning_stag()
#
# print("rule_3_male_evening_imrov")
# rule_3_male_evening_imrov()
# print("rule_3_female_morning_stag")
# rule_3_female_morning_stag()
# print("rule_3_female__morning_stag")
# rule_3_female__morning_stag()
# print("rule_3a_male_evening")
# rule_3a_male_evening()
# print("rule_3a_female_morning")
# rule_3a_female_morning()
# print("rule_3b_female_morning")
# rule_3b_female_morning()
# print("rule_4_female_morning")
# rule_4_female_morning()
# print("rule_4_male_evening")
# rule_4_male_evening()
# print("rule_4_female_morning")
# rule_4_female_morning()
# print("rule_5_female_morning_all_deter")
# rule_5_female_morning_all_deter()
# print("rule_5_female_morning_all_stag")
# rule_5_female_morning_all_stag()
#
# print("rule_5_male_evening_all_deter")
# rule_5_male_evening_all_deter()
# print("rule_5_male_evening_improv")
# rule_5_male_evening_improv()
print("rule_6_female_morning")
rule_6_female_morning()
print("rule_6_male_evening")
rule_6_male_evening()
print("rule_7_male_evening")
rule_7_male_evening()
print("rule_7_female_morning")
rule_7_female_morning()
print("rule_8_male_evening")
rule_8_male_evening()
print("rule_8_female_morning")
rule_8_female_morning()
print("rule_9_female_morning")
rule_9_female_morning()
print("rule_9_male_evening")
rule_9_male_evening()