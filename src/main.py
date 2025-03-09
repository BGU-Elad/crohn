from src.heuristics.human_expert_v3 import HumanExpert
from src.db_handler.xl_database import ExcelSQL

# Connect to SQLite database
# exec_sql = ExcelSQL(['../sensitive_data/data (32).xlsx', '../sensitive_data/checkWork -26.8.2024.xlsx', "../sensitive_data/acount and passwords.xlsx", "../sensitive_data/messages.xlsx"])
exec_sql = ExcelSQL(['../sensitive_data/debug/data (21).xlsx', '../sensitive_data/debug/checkWork -26.8.2024.xlsx', "../sensitive_data/debug/acount and passwords.xlsx", "../sensitive_data/debug/messages.xlsx"])
exec_sql.create_table()
# get_tables = exec_sql.get_tables()
# print(get_tables)
# get_tables = exec_sql.get_table("App_user")
# print(get_tables)


he = HumanExpert(exec_sql)
recommendations = he.recommend()

print(recommendations)
