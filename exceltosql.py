import pandas as pd
from sqlalchemy import create_engine
import logging

from sqlalchemy import create_engine, text
import pandas as pd

# Database connection details
engine = create_engine('mysql+pymysql://root:Narayan%4074@localhost/olympics')

# Load the Excel file
df = pd.read_excel(r"C:\Users\naray\DataEngineering\Medals.xlsx")

# Table name in your database
table_name = 'olympic_medals'

# Check if the table exists
with engine.connect() as connection:
    result = connection.execute(text("SHOW TABLES"))
    tables = [row[0] for row in result]
    if table_name not in tables:
        raise Exception(f"Table '{table_name}' does not exist in the database.")

# Insert data row-wise
with engine.connect() as connection:
    for index, row in df.iterrows():
        try:
            # Convert the row to a dictionary and use it in an INSERT statement
            row_data = row.to_dict()

            # Prepare SQL statement and parameters
            sql = f"""
                INSERT INTO {table_name} (Ranks, Team_NOC, Gold, Silver, Bronze, Total, Rank_by_Total)
                VALUES (:Rank, :Team_NOC, :Gold, :Silver, :Bronze, :Total, :Rank_by_Total)
            """
            params = {
                'Rank': row_data['Rank'],
                'Team_NOC': row_data['Team/NOC'],
                'Gold': row_data['Gold'],
                'Silver': row_data['Silver'],
                'Bronze': row_data['Bronze'],
                'Total': row_data['Total'],
                'Rank_by_Total': row_data['Rank by Total']
            }

            # Execute the SQL statement
            connection.execute(text(sql), params)

            print(f"Row {index + 1} inserted successfully.")
        except Exception as e:
            print(f"Error inserting row {index + 1}: {e}")

print("Data transfer completed.")
