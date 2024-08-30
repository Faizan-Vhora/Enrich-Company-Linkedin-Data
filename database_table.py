from sqlalchemy import text
from sqlalchemy import create_engine


connection_string = "sqlite:///linkedindata.db"
engine = create_engine(connection_string)
# Define the SQL statement to create the table
create_companies_table_sql = '''

CREATE TABLE IF NOT EXISTS companies (
             company_id INTEGER AUTO_INCREMENT PRIMARY KEY,
             company_name TEXT NOT NULL,
             company_linkedin_url NOT NULL
             );

'''

create_enriched_table_sql = """
CREATE TABLE IF NOT EXISTS enriched_company_data (
    company_id INTEGER PRIMARY KEY,
    company_linkedin_url TEXT,
    company_name TEXT,
    industry TEXT,
    employee_count INTEGER
);
"""
# Execute the SQL statement
with engine.connect() as connection:
    connection.execute(text(create_companies_table_sql))
    connection.execute(text(create_enriched_table_sql))
    print("Tables created successfully!")