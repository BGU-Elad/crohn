from src.heuristics.human_expert import HumanExpert
from src.db_handler.xl_database import ExcelSQL

# Connect to SQLite database
exec_sql = ExcelSQL(['../sensitive_data/data.xlsx', '../sensitive_data/checkWork - 25.06.23.xlsx'])
exec_sql.create_table()
# get_tables = exec_sql.get_tables()
# print(get_tables)
# get_tables = exec_sql.get_table("App_user")
# print(get_tables)


he = HumanExpert(exec_sql)
recommendations = he.recommend()
print(recommendations)
