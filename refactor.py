import re
import os

with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add import
if 'import db_wrapper' not in content:
    content = content.replace('import sqlite3', 'import sqlite3\nimport db_wrapper')

# 2. Change Exceptions
content = content.replace('sqlite3.IntegrityError', 'db_wrapper.IntegrityError')

# 3. Change def get_db_connection()
pattern_db = r'def get_db_connection\(\) -> sqlite3\.Connection:\s+conn = sqlite3\.connect\("users\.db"\)\s+conn\.row_factory = sqlite3\.Row\s+return conn'
repl_db = '''def get_db_connection():
    return db_wrapper.get_db_connection()'''
content = re.sub(pattern_db, repl_db, content)

# 4. Change def get_app_data_db_connection()
pattern_app = r'def get_app_data_db_connection\(\) -> sqlite3\.Connection:\s+"""Return a connection to the unified app_data\.db\."""\s+conn = sqlite3\.connect\(APP_DATA_DB_PATH\)\s+conn\.row_factory = sqlite3\.Row\s+return conn'
repl_app = '''def get_app_data_db_connection():
    """Return a connection to the unified app_data.db."""
    return db_wrapper.get_app_data_db_connection()'''
content = re.sub(pattern_app, repl_app, content)

# 5. init_db
pattern_init = r'def init_db\(\) -> None:\s+conn = sqlite3\.connect\("users\.db"\)'
repl_init = '''def init_db() -> None:
    conn = db_wrapper.get_db_connection()'''
content = re.sub(pattern_init, repl_init, content)

# 6. init_app_data_db
pattern_init2 = r'def init_app_data_db\(\) -> None:\s+"""Create all non-user tables in the unified app_data\.db\."""\s+os\.makedirs\(REPORTS_DIR, exist_ok=True\)\s+conn = sqlite3\.connect\(APP_DATA_DB_PATH\)'
repl_init2 = '''def init_app_data_db() -> None:
    """Create all non-user tables in the unified app_data.db."""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    conn = db_wrapper.get_app_data_db_connection()'''
content = re.sub(pattern_init2, repl_init2, content)

# 7. Type hints on fetches
content = content.replace('conn: sqlite3.Connection', 'conn')
content = content.replace('-> sqlite3.Connection', '')

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(content)

# And similarly for reciever.py
with open('reciever.py', 'r', encoding='utf-8') as f:
    rcontent = f.read()

if 'import db_wrapper' not in rcontent:
    rcontent = rcontent.replace('import sqlite3', 'import sqlite3\nimport db_wrapper')

pattern_r1 = r'conn = sqlite3\.connect\(APP_DATA_DB_PATH\)'
repl_r1 = 'conn = db_wrapper.get_app_data_db_connection()'
rcontent = re.sub(pattern_r1, repl_r1, rcontent)

with open('reciever.py', 'w', encoding='utf-8') as f:
    f.write(rcontent)

print("Refactor complete.")
