from datetime import datetime

from src.heuristics.human_expert_v3 import HumanExpert
from src.db_handler.xl_database import ExcelSQL
from src.utils.constants import MINUS_TIME
from src.utils.utils import get_now

# Connect to SQLite database
# exec_sql = ExcelSQL(['../sensitive_data/data (32).xlsx', '../sensitive_data/checkWork -26.8.2024.xlsx', "../sensitive_data/acount and passwords.xlsx", "../sensitive_data/messages.xlsx"])
exec_sql = ExcelSQL([
    "../sensitive_data/for_tal/DB QA - rule 3 26.6- Female  Morning STAG .xlsx",#"../sensitive_data/for_tal/user_data.xlsx", # '../sensitive_data/_DB QA 1 - to upload 23.06.2025.xlsx', #'../sensitive_data/data April 3rd 2025.xlsx',
    "../sensitive_data/for_tal/app_data.xlsx", # '../sensitive_data/debug/checkWork -26.8.2024.xlsx',
    "../sensitive_data/for_tal/acount and passwords.xlsx",#"../sensitive_data/debug/acount and passwords2.xlsx",
    "../sensitive_data/for_tal/messages.xlsx",    # "../sensitive_data/debug/messages.xlsx",
])
exec_sql.create_table()

# get_tables = exec_sql.get_tables()
# print(get_tables)
# get_tables = exec_sql.get_table("App_user")
# print(get_tables)


minus_days = (get_now(0).date() - datetime.strptime("26.06.2025", "%d.%m.%Y").date()).days
he = HumanExpert(exec_sql, minus_days)
recommendations = he.recommend()

print(recommendations)
