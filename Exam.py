# import pandas as pd
# import numpy as np
# from datetime import datetime

# # Read the CSV file
# df = pd.read_csv(r'c:\Users\user\Desktop\RewardsData.csv')

# ## 1. Delete the tags, Joined On, and Last Seen columns
# df = df.drop(columns=['Tags', 'Joined On', 'Last Seen'], errors='ignore')

# ## 2. Fill empty cell in row 438 (Python uses 0-based index, so row 437) with 11011
# if len(df) > 437:
#     df.at[437, 'Zip'] = '11011'

# ## 3. Keep only first 5 digits in zip codes
# df['Zip'] = df['Zip'].astype(str).str.extract(r'^(\d{5})')[0]

# ## 4. Calculate mean zip code (as integer) and fill empty cells
# valid_zips = pd.to_numeric(df['Zip'], errors='coerce')
# mean_zip = int(valid_zips.mean())
# df['Zip'] = df['Zip'].fillna(str(mean_zip))

# ## 5. Standardize "Winston Salem" capitalization
# df['City'] = df['City'].str.replace('winston salem', 'Winston Salem', case=False)
# df['City'] = df['City'].str.replace('winston-salem', 'Winston Salem', case=False)

# ## 6. Remove single-letter city abbreviations (replace with empty)
# df['City'] = df['City'].apply(lambda x: np.nan if isinstance(x, str) and len(x.strip()) == 1 else x)

# ## 7. Replace state abbreviations with full names
# state_mapping = {
#     'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
#     'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
#     'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
#     'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
#     'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
#     'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
#     'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
#     'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
#     'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
#     'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
#     'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
#     'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
#     'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia'
# }
# df['State'] = df['State'].replace(state_mapping)

# ## 8. Fill empty state cells with states in alphabetical order
# all_states = sorted(state_mapping.values())
# empty_state_count = df['State'].isna().sum()
# if empty_state_count > 0:
#     states_to_fill = all_states * (empty_state_count // len(all_states)) + all_states[:empty_state_count % len(all_states)]
#     df.loc[df['State'].isna(), 'State'] = states_to_fill

# ## 9. Standardize birthdate format
# df['Birthdate'] = pd.to_datetime(df['Birthdate'], errors='coerce').dt.strftime('%m/%d/%Y')

# ## 10. Fill empty birthdates with a specific date (e.g., 01/01/1990)
# df['Birthdate'] = df['Birthdate'].fillna('01/01/1990')

# ## 11. Remove rows with zip codes not exactly 5 digits
# df = df[df['Zip'].astype(str).str.len() == 5]

# ## 12. Fill empty city cells with "Thomas Ville"
# df['City'] = df['City'].fillna('Thomas Ville')

# # Clean remaining columns
# df['User ID'] = df['User ID'].astype(int)
# df['Available Points'] = pd.to_numeric(df['Available Points'], errors='coerce')
# df['Total Points Earned'] = pd.to_numeric(df['Total Points Earned'], errors='coerce')
# df['Points Spent'] = pd.to_numeric(df['Points Spent'], errors='coerce')

# # Save cleaned data
# df.to_csv('cleaned_rewards_data.csv', index=False)

# print("Data cleaning complete. File saved as 'cleaned_rewards_data.csv'")



# import psycopg2
# conn = psycopg2.connect(
#     host="localhost",
#     dbname="MyRewardData",
#     user="postgres",
#     password="3@Jollyboy"
# )
# cur = conn.cursor()
# cur.execute("""
#     CREATE TABLE IF NOT EXISTS rewards_data (
#         User_id INTEGER PRIMARY KEY,
#         Birthdate DATE,
#         City VARCHAR(100),
#         State VARCHAR(50),
#         Zip VARCHAR(10),
#         Available_Points INTEGER,
#         Total_Points_Earned INTEGER,
#         Points_Spent INTEGER
#     )
# """)
# conn.commit()
# cur.close()
# conn.close()
# print("Table created successfully!")


# import csv
# import psycopg2
# from io import StringIO

# def copy_from_csv(conn, table_name, csv_file_path, delimiter=',', null_string=''):
#     """
#     Copies data from a CSV file into a PostgreSQL table using the COPY command.
#     This is the most efficient way to load large amounts of data.

#     Args:
#         conn: psycopg2 connection object.
#         table_name (str): The name of the table to copy data into.
#         csv_file_path (str): Path to the CSV file.
#         delimiter (str, optional): The delimiter used in the CSV file. Defaults to ','.
#         null_string (str, optional): The string representing NULL values in the CSV. Defaults to ''.
#     """
#     cursor = conn.cursor()
#     try:
#         with open(csv_file_path, 'r') as f:
#             # Use csv.Sniffer to detect the dialect
#             dialect = csv.Sniffer().sniff(f.read(1024))
#             f.seek(0)
#             reader = csv.reader(f, dialect)
#             header = [col.strip() for col in next(reader)]  # Clean header names
            
#             csv_file = StringIO()
#             writer = csv.writer(csv_file, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
#             for row in reader:
#                 if len(row) > len(header):
#                     row = row[:len(header)]
#                 elif len(row) < len(header):
#                     row += [null_string] * (len(header) - len(row))
#                 writer.writerow(row)
            
#             csv_file.seek(0)
            
#             # Use quoted column names in the COPY command
#             quoted_columns = ','.join([f'"{col}"' for col in header])
#             cursor.copy_expert(f"COPY {table_name} ({quoted_columns}) FROM STDIN WITH CSV NULL '{null_string}' DELIMITER '{delimiter}'", csv_file)
            
#             conn.commit()
#             print(f"Data from '{csv_file_path}' successfully copied to table '{table_name}'.")

#     except psycopg2.Error as e:
#         print(f"Error copying data from CSV to table: {e}")
#         conn.rollback()
#         raise
#     except Exception as e:
#         print(f"Error processing CSV file: {e}")
#         conn.rollback()
#         raise
#     finally:
#         cursor.close()

# def create_table_from_csv(conn, table_name, csv_file_path, null_string=''):
#     """
#     Creates a PostgreSQL table from a CSV file, with proper handling of column names with spaces.
#     """
#     cursor = conn.cursor()
#     try:
#         with open(csv_file_path, 'r') as f:
#             # Detect CSV dialect
#             dialect = csv.Sniffer().sniff(f.read(1024))
#             f.seek(0)
#             reader = csv.reader(f, dialect)
            
#             # Clean and quote column names
#             header = [col.strip() for col in next(reader)]
#             quoted_header = [f'"{col}"' for col in header]
            
#             # Get first data row for type inference
#             first_data_row = next(reader, None)
#             if not first_data_row:
#                 raise ValueError("CSV file has no data rows")
            
#             # Infer column types
#             column_types = []
#             for value in first_data_row:
#                 if value == null_string:
#                     column_types.append('TEXT')
#                 else:
#                     try:
#                         int(value)
#                         column_types.append('INTEGER')
#                     except ValueError:
#                         try:
#                             float(value)
#                             column_types.append('REAL')
#                         except ValueError:
#                             column_types.append('TEXT')
            
#             # Build CREATE TABLE statement with quoted column names
#             columns = [f'{quoted_header[i]} {column_types[i]}' for i in range(len(header))]
#             create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
            
#             cursor.execute(create_table_query)
#             conn.commit()
#             print(f"Table '{table_name}' created successfully with columns: {', '.join(quoted_header)}")

#     except psycopg2.Error as e:
#         print(f"Database error creating table: {e}")
#         conn.rollback()
#         raise
#     except Exception as e:
#         print(f"Error processing CSV file: {e}")
#         conn.rollback()
#         raise
#     finally:
#         cursor.close()

# def main():
#     """
#     Main function to connect to the database, create a table, and copy data from a CSV file.
#     """
#     # Database connection details
#     dbname = "my_Database"
#     user = "postgres"
#     password = "3@Jollyboy"
#     host = "localhost"
#     port = "5432"

#     # CSV file path and table name
#     csv_file_path = r"c:\Users\user\Desktop\film1.csv"
#     table_name = "film"

#     conn = None
#     try:
#         # Establish database connection
#         conn = psycopg2.connect(
#             dbname=dbname,
#             user=user,
#             password=password,
#             host=host,
#             port=port
#         )
#         conn.autocommit = False

#         # Create the table
#         create_table_from_csv(conn, table_name, csv_file_path)

#         # Copy data from CSV
#         copy_from_csv(conn, table_name, csv_file_path)

#         conn.commit()
#         print("Data loading completed successfully.")

#     except Exception as e:
#         print(f"An error occurred: {e}")
#         if conn:
#             conn.rollback()
#     finally:
#         if conn:
#             conn.close()
#             print("Database connection closed.")

# if __name__ == "__main__":
#     main()

# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import sqlalchemy
# from sqlalchemy import create_engine, text
# import logging
# from datetime import datetime

# # Configure logging
# logging.basicConfig(
#     filename='bank_scraping_log.txt',
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

# # Constants
# URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
# EXCHANGE_RATES = {
#     'euro': 0.93,
#     'pound': 0.8,
#     'INR': 82.95
# }
# DATABASE_NAME = "banks_db"
# TABLE_NAME = "largest_banks"

# def scrape_bank_data():
#     """Scrape bank data from Wikipedia and process it."""
#     try:
#         logging.info("Starting web scraping process")
        
#         # Extract HTML and create BeautifulSoup object
#         html_data = requests.get(URL).text
#         soup = BeautifulSoup(html_data, 'html.parser')
        
#         # Find the table
#         tables = soup.find_all('table')
#         bank_table = tables[0]  # Assuming the first table is the correct one
        
#         # Extract data
#         data = []
#         for row in bank_table.tbody.find_all('tr'):
#             cols = row.find_all('td')
#             if cols:
#                 bank_name = cols[1].text.strip()
#                 market_cap = float(cols[2].text.strip().replace('\n', '').replace(',', ''))
#                 data.append({'Bank Name': bank_name, 'Market Cap (USD)': market_cap})
        
#         # Create DataFrame
#         df = pd.DataFrame(data)
        
#         # Calculate other currencies
#         for currency, rate in EXCHANGE_RATES.items():
#             df[f'Market Cap ({currency.upper()})'] = df['Market Cap (USD)'] * rate
        
#         # Round to 2 decimal places
#         for col in df.columns[1:]:
#             df[col] = df[col].round(2)
        
#         logging.info("Data successfully scraped and processed")
#         return df
    
#     except Exception as e:
#         logging.error(f"Error during scraping: {str(e)}")
#         raise

# def setup_database(df):
#     """Set up database and load data."""
#     try:
#         logging.info("Setting up database connection")
        
#         # Create engine (PostgreSQL)
#         engine = create_engine(f'postgresql://postgres:postgres@localhost:5432/postgres')
        
#         # Check if database exists, create if not
#         with engine.connect() as conn:
#             result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :dbname"), 
#                                 {'dbname': DATABASE_NAME})
#             if not result.scalar():
#                 conn.execute(text(f"CREATE DATABASE {DATABASE_NAME}"))
#                 logging.info(f"Database {DATABASE_NAME} created")
#             else:
#                 logging.info(f"Database {DATABASE_NAME} already exists")
        
#         # Connect to the specific database
#         engine = create_engine(f'postgresql://postgres:postgres@localhost:5432/{DATABASE_NAME}')
        
#         # Load data to database
#         df.to_sql(
#             name=TABLE_NAME,
#             con=engine,
#             if_exists='replace',
#             index=False,
#             dtype={
#                 'Bank Name': sqlalchemy.types.VARCHAR(length=255),
#                 'Market Cap (USD)': sqlalchemy.types.FLOAT,
#                 'Market Cap (EURO)': sqlalchemy.types.FLOAT,
#                 'Market Cap (POUND)': sqlalchemy.types.FLOAT,
#                 'Market Cap (INR)': sqlalchemy.types.FLOAT
#             }
#         )
        
#         logging.info("Data successfully loaded to database")
        
#     except Exception as e:
#         logging.error(f"Error during database operations: {str(e)}")
#         raise

# def main():
#     """Main function to execute the process."""
#     try:
#         logging.info("Process started")
        
#         # Scrape and process data
#         bank_data = scrape_bank_data()
        
#         # Set up database and load data
#         setup_database(bank_data)
        
#         logging.info("Process completed successfully")
        
#     except Exception as e:
#         logging.error(f"Process failed: {str(e)}")

# if __name__ == "__main__":
#     main()

# import pandas as pd
# import sqlalchemy

# # Read both tables
# engine = sqlalchemy.create_engine('postgresql://username:password@localhost:5432/database_name')
# df_source = pd.read_sql('SELECT * FROM film', engine)
# df_target = pd.read_sql('SELECT * FROM film1', engine)

# # Transfer columns
# df_target['new_column'] = df_source['source_column']

# # Write back to database
# df_target.to_sql('target_table', engine, if_exists='replace', index=False)


# import pandas as pd
# import psycopg2
# from sqlalchemy import create_engine, inspect, text
# import logging
# from tkinter import filedialog, messagebox
# import tkinter as tk
# from tkinter import ttk
# import os
# from datetime import datetime

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('csv_to_postgres.log'),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)

# class CSVtoPostgresLoader:
#     def __init__(self, root):
#         self.root = root
#         self.root.title("CSV to PostgreSQL Loader")
#         self.root.geometry("800x600")
        
#         # Database configuration
#         self.db_config = {
#             'host': 'localhost',
#             'database': 'BankApp',
#             'user': 'postgres',
#             'password': '3@Jollyboy',  # Change to your PostgreSQL password
#             'port': '5432'
#         }
        
#         # CSV file path
#         self.csv_file_path = None
        
#         # Create UI
#         self.create_widgets()
        
#     def create_widgets(self):
#         """Create the GUI interface"""
#         main_frame = ttk.Frame(self.root, padding="20")
#         main_frame.pack(fill=tk.BOTH, expand=True)
        
#         # Title
#         title_label = ttk.Label(
#             main_frame, 
#             text="CSV to PostgreSQL Data Loader",
#             font=('Helvetica', 16, 'bold')
#         )
#         title_label.grid(row=0, column=0, columnspan=2, pady=10)
        
#         # CSV File Selection
#         ttk.Label(main_frame, text="CSV File:").grid(row=1, column=0, sticky=tk.W)
#         self.csv_path_label = ttk.Label(main_frame, text="No file selected")
#         self.csv_path_label.grid(row=1, column=1, sticky=tk.W)
#         ttk.Button(
#             main_frame, 
#             text="Browse", 
#             command=self.select_csv_file
#         ).grid(row=1, column=2, padx=5)
        
#         # Database Configuration
#         ttk.Label(main_frame, text="Database Configuration", font=('Helvetica', 12)).grid(row=2, column=0, columnspan=3, pady=10, sticky=tk.W)
        
#         config_frame = ttk.Frame(main_frame)
#         config_frame.grid(row=3, column=0, columnspan=3, sticky=tk.W)
        
#         ttk.Label(config_frame, text="Host:").grid(row=0, column=0, sticky=tk.W)
#         self.host_entry = ttk.Entry(config_frame)
#         self.host_entry.grid(row=0, column=1, padx=5, pady=2)
#         self.host_entry.insert(0, self.db_config['host'])
        
#         ttk.Label(config_frame, text="Database:").grid(row=1, column=0, sticky=tk.W)
#         self.db_entry = ttk.Entry(config_frame)
#         self.db_entry.grid(row=1, column=1, padx=5, pady=2)
#         self.db_entry.insert(0, self.db_config['database'])
        
#         ttk.Label(config_frame, text="User:").grid(row=2, column=0, sticky=tk.W)
#         self.user_entry = ttk.Entry(config_frame)
#         self.user_entry.grid(row=2, column=1, padx=5, pady=2)
#         self.user_entry.insert(0, self.db_config['user'])
        
#         ttk.Label(config_frame, text="Password:").grid(row=3, column=0, sticky=tk.W)
#         self.pass_entry = ttk.Entry(config_frame, show="*")
#         self.pass_entry.grid(row=3, column=1, padx=5, pady=2)
#         self.pass_entry.insert(0, self.db_config['password'])
        
#         ttk.Label(config_frame, text="Port:").grid(row=4, column=0, sticky=tk.W)
#         self.port_entry = ttk.Entry(config_frame)
#         self.port_entry.grid(row=4, column=1, padx=5, pady=2)
#         self.port_entry.insert(0, self.db_config['port'])
        
#         # Table Options
#         ttk.Label(main_frame, text="Table Options", font=('Helvetica', 12)).grid(row=4, column=0, columnspan=3, pady=10, sticky=tk.W)
        
#         ttk.Label(main_frame, text="Table Name:").grid(row=5, column=0, sticky=tk.W)
#         self.table_entry = ttk.Entry(main_frame)
#         self.table_entry.grid(row=5, column=1, sticky=tk.W, padx=5)
#         self.table_entry.insert(0, "csv_import_data")
        
#         self.if_exists_var = tk.StringVar(value="replace")
#         ttk.Label(main_frame, text="If Table Exists:").grid(row=6, column=0, sticky=tk.W)
#         ttk.Radiobutton(
#             main_frame, 
#             text="Replace", 
#             variable=self.if_exists_var, 
#             value="replace"
#         ).grid(row=6, column=1, sticky=tk.W)
#         ttk.Radiobutton(
#             main_frame, 
#             text="Append", 
#             variable=self.if_exists_var, 
#             value="append"
#         ).grid(row=6, column=2, sticky=tk.W)
        
#         # Load Button
#         self.load_button = ttk.Button(
#             main_frame, 
#             text="Load CSV to PostgreSQL", 
#             command=self.load_csv_to_postgres,
#             state=tk.DISABLED
#         )
#         self.load_button.grid(row=7, column=0, columnspan=3, pady=20)
        
#         # Progress Bar
#         self.progress = ttk.Progressbar(main_frame, orient=tk.HORIZONTAL, length=400, mode='determinate')
#         self.progress.grid(row=8, column=0, columnspan=3, pady=10)
        
#         # Status Label
#         self.status_label = ttk.Label(main_frame, text="Ready", relief=tk.SUNKEN, anchor=tk.W)
#         self.status_label.grid(row=9, column=0, columnspan=3, sticky=tk.EW)
        
#         # Configure grid weights
#         main_frame.columnconfigure(1, weight=1)
        
#     def select_csv_file(self):
#         """Select CSV file dialog"""
#         file_path = filedialog.askopenfilename(
#             title="Select CSV File",
#             filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
#         )
#         if file_path:
#             self.csv_file_path = file_path
#             self.csv_path_label.config(text=os.path.basename(file_path))
#             self.load_button['state'] = tk.NORMAL
#             logger.info(f"Selected CSV file: {file_path}")
#             self.update_status(f"Selected: {os.path.basename(file_path)}")
    
#     def update_status(self, message):
#         """Update status label"""
#         self.status_label.config(text=message)
#         self.root.update_idletasks()
    
#     def get_db_connection(self):
#         """Create and return a database connection"""
#         try:
#             engine = create_engine(
#                 f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
#                 f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
#             )
#             return engine
#         except Exception as e:
#             logger.error(f"Database connection failed: {str(e)}")
#             raise
    
#     def load_csv_to_postgres(self):
#         """Main function to load CSV data into PostgreSQL"""
#         if not self.csv_file_path:
#             messagebox.showerror("Error", "No CSV file selected")
#             return
        
#         try:
#             # Update UI
#             self.update_status("Loading CSV data...")
#             self.progress['value'] = 10
#             self.root.update_idletasks()
            
#             # Get user inputs
#             self.db_config = {
#                 'host': self.host_entry.get(),
#                 'database': self.db_entry.get(),
#                 'user': self.user_entry.get(),
#                 'password': self.pass_entry.get(),
#                 'port': self.port_entry.get()
#             }
#             table_name = self.table_entry.get()
#             if_exists = self.if_exists_var.get()
            
#             logger.info(f"Starting CSV import to table: {table_name}")
            
#             # Step 1: Read CSV file
#             self.update_status("Reading CSV file...")
#             self.progress['value'] = 20
#             self.root.update_idletasks()
            
#             try:
#                 df = pd.read_csv(self.csv_file_path)
#                 logger.info(f"CSV file read successfully. Rows: {len(df)}")
#             except Exception as e:
#                 logger.error(f"Error reading CSV file: {str(e)}")
#                 raise
            
#             # Step 2: Clean column names
#             self.update_status("Cleaning data...")
#             self.progress['value'] = 30
#             self.root.update_idletasks()
            
#             # Standardize column names
#             df.columns = df.columns.str.strip()
#             df.columns = df.columns.str.lower()
#             df.columns = df.columns.str.replace(r'[^\w]', '_', regex=True)
#             df.columns = df.columns.str.replace(r'_+', '_', regex=True)
            
#             # Step 3: Connect to database
#             self.update_status("Connecting to database...")
#             self.progress['value'] = 40
#             self.root.update_idletasks()
            
#             try:
#                 engine = self.get_db_connection()
#                 logger.info("Database connection established")
#             except Exception as e:
#                 logger.error(f"Database connection error: {str(e)}")
#                 raise
            
#             # Step 4: Load data to PostgreSQL
#             self.update_status("Loading data to PostgreSQL...")
#             self.progress['value'] = 60
#             self.root.update_idletasks()
            
#             try:
#                 # Get SQLAlchemy connection
#                 with engine.connect() as conn:
#                     # Create table if needed
#                     if if_exists == 'replace':
#                         conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
#                         conn.commit()
#                         logger.info(f"Dropped existing table: {table_name}")
                    
#                     # Load data
#                     df.to_sql(
#                         name=table_name,
#                         con=engine,
#                         if_exists=if_exists,
#                         index=False,
#                         method='multi'
#                     )
                
#                 logger.info(f"Data loaded successfully to table: {table_name}")
#                 self.progress['value'] = 100
#                 self.update_status(f"Data loaded to {table_name} successfully!")
#                 messagebox.showinfo(
#                     "Success", 
#                     f"CSV data loaded to PostgreSQL table '{table_name}' successfully!\n"
#                     f"Rows inserted: {len(df)}"
#                 )
                
#             except Exception as e:
#                 logger.error(f"Error loading data to PostgreSQL: {str(e)}")
#                 raise
            
#         except Exception as e:
#             logger.error(f"CSV import failed: {str(e)}")
#             self.progress['value'] = 0
#             self.update_status(f"Error: {str(e)}")
#             messagebox.showerror(
#                 "Error", 
#                 f"Failed to load CSV data:\n{str(e)}"
#             )
        
#         finally:
#             try:
#                 engine.dispose()
#                 logger.info("Database connection closed")
#             except:
#                 pass

# if __name__ == "__main__":
#     root = tk.Tk()
#     app = CSVtoPostgresLoader(root)
#     root.mainloop()

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, inspect, text
import logging
from tkinter import filedialog, messagebox
import tkinter as tk
from tkinter import ttk
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('csv_to_postgres.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CSVtoPostgresLoader:
    def __init__(self, root):
        self.root = root
        self.root.title("CSV to PostgreSQL Loader")
        self.root.geometry("800x600")
        
        # Database configuration
        self.db_config = {
            'host': 'localhost',
            'database': 'BankApp',
            'user': 'postgres',
            'password': '3@Jollyboy',  # Change to your PostgreSQL password
            'port': '5432'
        }
        
        # CSV file path
        self.csv_file_path = None
        
        # Create UI
        self.create_widgets()
        
    def create_widgets(self):
        """Create the GUI interface"""
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(
            main_frame, 
            text="CSV to PostgreSQL Data Loader",
            font=('Helvetica', 16, 'bold')
        )
        title_label.grid(row=0, column=0, columnspan=2, pady=10)
        
        # CSV File Selection
        ttk.Label(main_frame, text="CSV File:").grid(row=1, column=0, sticky=tk.W)
        self.csv_path_label = ttk.Label(main_frame, text="No file selected")
        self.csv_path_label.grid(row=1, column=1, sticky=tk.W)
        ttk.Button(
            main_frame, 
            text="Browse", 
            command=self.select_csv_file
        ).grid(row=1, column=2, padx=5)
        
        # Database Configuration
        ttk.Label(main_frame, text="Database Configuration", font=('Helvetica', 12)).grid(row=2, column=0, columnspan=3, pady=10, sticky=tk.W)
        
        config_frame = ttk.Frame(main_frame)
        config_frame.grid(row=3, column=0, columnspan=3, sticky=tk.W)
        
        ttk.Label(config_frame, text="Host:").grid(row=0, column=0, sticky=tk.W)
        self.host_entry = ttk.Entry(config_frame)
        self.host_entry.grid(row=0, column=1, padx=5, pady=2)
        self.host_entry.insert(0, self.db_config['host'])
        
        ttk.Label(config_frame, text="Database:").grid(row=1, column=0, sticky=tk.W)
        self.db_entry = ttk.Entry(config_frame)
        self.db_entry.grid(row=1, column=1, padx=5, pady=2)
        self.db_entry.insert(0, self.db_config['database'])
        
        ttk.Label(config_frame, text="User:").grid(row=2, column=0, sticky=tk.W)
        self.user_entry = ttk.Entry(config_frame)
        self.user_entry.grid(row=2, column=1, padx=5, pady=2)
        self.user_entry.insert(0, self.db_config['user'])
        
        ttk.Label(config_frame, text="Password:").grid(row=3, column=0, sticky=tk.W)
        self.pass_entry = ttk.Entry(config_frame, show="*")
        self.pass_entry.grid(row=3, column=1, padx=5, pady=2)
        self.pass_entry.insert(0, self.db_config['password'])
        
        ttk.Label(config_frame, text="Port:").grid(row=4, column=0, sticky=tk.W)
        self.port_entry = ttk.Entry(config_frame)
        self.port_entry.grid(row=4, column=1, padx=5, pady=2)
        self.port_entry.insert(0, self.db_config['port'])
        
        # Table Options
        ttk.Label(main_frame, text="Table Options", font=('Helvetica', 12)).grid(row=4, column=0, columnspan=3, pady=10, sticky=tk.W)
        
        ttk.Label(main_frame, text="Table Name:").grid(row=5, column=0, sticky=tk.W)
        self.table_entry = ttk.Entry(main_frame)
        self.table_entry.grid(row=5, column=1, sticky=tk.W, padx=5)
        self.table_entry.insert(0, "csv_import_data")
        
        self.if_exists_var = tk.StringVar(value="replace")
        ttk.Label(main_frame, text="If Table Exists:").grid(row=6, column=0, sticky=tk.W)
        ttk.Radiobutton(
            main_frame, 
            text="Replace", 
            variable=self.if_exists_var, 
            value="replace"
        ).grid(row=6, column=1, sticky=tk.W)
        ttk.Radiobutton(
            main_frame, 
            text="Append", 
            variable=self.if_exists_var, 
            value="append"
        ).grid(row=6, column=2, sticky=tk.W)
        
        # Load Button
        self.load_button = ttk.Button(
            main_frame, 
            text="Load CSV to PostgreSQL", 
            command=self.load_csv_to_postgres,
            state=tk.DISABLED
        )
        self.load_button.grid(row=7, column=0, columnspan=3, pady=20)
        
        # Progress Bar
        self.progress = ttk.Progressbar(main_frame, orient=tk.HORIZONTAL, length=400, mode='determinate')
        self.progress.grid(row=8, column=0, columnspan=3, pady=10)
        
        # Status Label
        self.status_label = ttk.Label(main_frame, text="Ready", relief=tk.SUNKEN, anchor=tk.W)
        self.status_label.grid(row=9, column=0, columnspan=3, sticky=tk.EW)
        
        # Configure grid weights
        main_frame.columnconfigure(1, weight=1)
        
    def select_csv_file(self):
        """Select CSV file dialog"""
        file_path = filedialog.askopenfilename(
            title="Select CSV File",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
        )
        if file_path:
            self.csv_file_path = file_path
            self.csv_path_label.config(text=os.path.basename(file_path))
            self.load_button['state'] = tk.NORMAL
            logger.info(f"Selected CSV file: {file_path}")
            self.update_status(f"Selected: {os.path.basename(file_path)}")
    
    def update_status(self, message):
        """Update status label"""
        self.status_label.config(text=message)
        self.root.update_idletasks()
    
    def get_db_connection(self):
        """Create and return a database connection"""
        try:
            engine = create_engine(
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
                f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )
            return engine
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise
    
    def load_csv_to_postgres(self):
        """Main function to load CSV data into PostgreSQL"""
        if not self.csv_file_path:
            messagebox.showerror("Error", "No CSV file selected")
            return
        
        try:
            # Update UI
            self.update_status("Loading CSV data...")
            self.progress['value'] = 10
            self.root.update_idletasks()
            
            # Get user inputs
            self.db_config = {
                'host': self.host_entry.get(),
                'database': self.db_entry.get(),
                'user': self.user_entry.get(),
                'password': self.pass_entry.get(),
                'port': self.port_entry.get()
            }
            table_name = self.table_entry.get()
            if_exists = self.if_exists_var.get()
            
            logger.info(f"Starting CSV import to table: {table_name}")
            
            # Step 1: Read CSV file
            self.update_status("Reading CSV file...")
            self.progress['value'] = 20
            self.root.update_idletasks()
            
            try:
                df = pd.read_csv(self.csv_file_path)
                logger.info(f"CSV file read successfully. Rows: {len(df)}")
            except Exception as e:
                logger.error(f"Error reading CSV file: {str(e)}")
                raise
            
            # Step 2: Clean column names
            self.update_status("Cleaning data...")
            self.progress['value'] = 30
            self.root.update_idletasks()
            
            # Standardize column names
            df.columns = df.columns.str.strip()
            df.columns = df.columns.str.lower()
            df.columns = df.columns.str.replace(r'[^\w]', '_', regex=True)
            df.columns = df.columns.str.replace(r'_+', '_', regex=True)
            
            # Step 3: Connect to database
            self.update_status("Connecting to database...")
            self.progress['value'] = 40
            self.root.update_idletasks()
            
            try:
                engine = self.get_db_connection()
                logger.info("Database connection established")
            except Exception as e:
                logger.error(f"Database connection error: {str(e)}")
                raise
            
            # Step 4: Load data to PostgreSQL
            self.update_status("Loading data to PostgreSQL...")
            self.progress['value'] = 60
            self.root.update_idletasks()
            
            try:
                # Get SQLAlchemy connection
                with engine.connect() as conn:
                    # Create table if needed
                    if if_exists == 'replace':
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                        conn.commit()
                        logger.info(f"Dropped existing table: {table_name}")
                    
                    # Load data
                    df.to_sql(
                        name=table_name,
                        con=engine,
                        if_exists=if_exists,
                        index=False,
                        method='multi'
                    )
                
                logger.info(f"Data loaded successfully to table: {table_name}")
                self.progress['value'] = 100
                self.update_status(f"Data loaded to {table_name} successfully!")
                messagebox.showinfo(
                    "Success", 
                    f"CSV data loaded to PostgreSQL table '{table_name}' successfully!\n"
                    f"Rows inserted: {len(df)}"
                )
                
            except Exception as e:
                logger.error(f"Error loading data to PostgreSQL: {str(e)}")
                raise
            
        except Exception as e:
            logger.error(f"CSV import failed: {str(e)}")
            self.progress['value'] = 0
            self.update_status(f"Error: {str(e)}")
            messagebox.showerror(
                "Error", 
                f"Failed to load CSV data:\n{str(e)}"
            )
        
        finally:
            try:
                engine.dispose()
                logger.info("Database connection closed")
            except:
                pass

if __name__ == "__main__":
    root = tk.Tk()
    app = CSVtoPostgresLoader(root)
    root.mainloop()

