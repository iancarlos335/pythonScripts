import pandas as pd
import pyodbc
import os
import glob

# --- CONFIGURATION ---
# DATABASE CONNECTION DETAILS
db_server = 'SERVER'      # Replace with your SQL Server name
db_database = 'DATABASE'  # Replace with your database name
# table_name will be derived from the CSV filename

# FOLDER CONFIGURATION
input_csv_folder = 'CSV_INPUT_FOLDER'     # Folder containing your source .csv files
output_sql_folder = 'SQL_OUTPUT_FOLDER'   # Folder where generated .sql files will be saved

# --- OPERATION MODE ---
# Set to 'INSERT' to generate INSERT statements
# Set to 'UPDATE' to generate UPDATE statements
operation_mode = 'INSERT'  # <<< CHANGE THIS TO 'INSERT' or 'UPDATE' as needed

# --- FOR UPDATE OPERATIONS ONLY ---
# Specify the primary key column name used in the WHERE clause for UPDATEs.
# This column *must* exist in your CSV files AND in the DB table for UPDATE operations.
primary_key_column = 'ID' # <<< CHANGE THIS to your actual primary key column name

# --- HELPER FUNCTION FOR SAFE SQL STRING CONVERSION ---
def safe_sql_string_converter(value_from_cell):
    if pd.isna(value_from_cell):
        return 'NULL'
    try:
        string_representation = str(value_from_cell)
    except Exception as e:
        print(f"Warning: Failed to convert value '{value_from_cell}' (type: {type(value_from_cell)}) to string due to: {e}. Treating as NULL.")
        return 'NULL'
    if string_representation is None: # Should not happen with str() but defensive
        return 'NULL'
    escaped_string = string_representation.replace("'", "''")
    return f"'{escaped_string}'"

# --- FUNCTION TO GET ALL COLUMNS AND DATE COLUMNS FOR A SPECIFIC TABLE ---
def get_table_schema_info(table_name_param, db_conn):
    """
    Queries the database for all columns and date type columns of a specific table.
    Returns a tuple: (list_of_all_uppercase_column_names, list_of_uppercase_date_type_column_names).
    """
    all_db_columns = []
    date_db_columns = []
    cursor = None 
    try:
        cursor = db_conn.cursor()
        # Query to get column names and their data types for the specified table
        schema_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name_param}'
        ORDER BY ORDINAL_POSITION; 
        """
        cursor.execute(schema_query)
        schema_details = cursor.fetchall()
        
        # Define list of SQL Server date types to check against
        date_type_list_sql = ['date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset']
        
        for detail in schema_details:
            col_name_upper = detail.COLUMN_NAME.upper()
            all_db_columns.append(col_name_upper)
            # Check if the column's data type is one of the defined date types
            if detail.DATA_TYPE.lower() in date_type_list_sql:
                date_db_columns.append(col_name_upper)
        
        if all_db_columns:
            if date_db_columns:
                print(f"    DB Schema for table '{table_name_param}': Date columns ({len(date_db_columns)}): {date_db_columns}")
        else:
            # This means the table might not exist or has no columns, which is unusual.
            print(f"    Warning: No columns found for table '{table_name_param}' in schema, or table does not exist.")
            
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"    Warning: Database query error while fetching schema for table '{table_name_param}': {sqlstate}.")
        print(f"    Proceeding as if table '{table_name_param}' has no specific date types or columns known from DB.")
    except Exception as e:
        print(f"    An unexpected error occurred while fetching schema for table '{table_name_param}': {e}")
    finally:
        if cursor:
            cursor.close()
    return all_db_columns, date_db_columns


# --- MAIN PROCESSING ---
def process_csv_files():
    """
    Reads all CSV files from the input folder, processes them, 
    and generates corresponding SQL files in the output folder.
    The table name is derived from the CSV filename.
    Only columns existing in both CSV and DB table are used.
    """
    if not os.path.exists(output_sql_folder):
        try:
            os.makedirs(output_sql_folder)
            print(f"Created output folder: {output_sql_folder}")
        except OSError as e:
            print(f"Error: Could not create output folder '{output_sql_folder}': {e}")
            return

    csv_files_pattern = os.path.join(input_csv_folder, '*.csv')
    csv_files = glob.glob(csv_files_pattern)

    if not csv_files:
        print(f"No CSV files found in '{input_csv_folder}'.")
        return

    print(f"Found {len(csv_files)} CSV files to process.")
    total_files_processed_successfully = 0
    db_conn = None
    try:
        print("Attempting to connect to the database...")
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server};DATABASE={db_database};Trusted_Connection=yes'
        db_conn = pyodbc.connect(connection_string)
        print("Database connection successful.")
        print("-" * 30)

        for csv_file_path in csv_files:
            base_filename_with_ext = os.path.basename(csv_file_path)
            current_table_name = os.path.splitext(base_filename_with_ext)[0]
            sql_filename = current_table_name + '.sql'
            output_sql_file_path = os.path.join(output_sql_folder, sql_filename)
            
            print(f"\nProcessing '{base_filename_with_ext}' for table '{current_table_name}' -> SQL: '{sql_filename}'...")

            # Get all DB columns and date DB columns for the current table
            all_db_columns_for_current_table, date_type_db_columns_for_current_table = get_table_schema_info(current_table_name, db_conn)

            if not all_db_columns_for_current_table:
                print(f"  Skipping CSV '{base_filename_with_ext}' as no schema information (no columns) was retrieved for table '{current_table_name}' from the database.")
                with open(output_sql_file_path, 'w', encoding='utf-8') as file:
                    file.write(f"-- Skipped: No database schema (no columns) found for table '{current_table_name}'.\n")
                continue 

            df = None
            try:
                df = pd.read_csv(csv_file_path, dtype=str, delimiter=';') 
            except FileNotFoundError:
                print(f"  Error: CSV file not found at {csv_file_path}. Skipping.")
                with open(output_sql_file_path, 'w', encoding='utf-8') as file: file.write(f"-- Error: CSV file not found at {csv_file_path}.\n")
                continue
            except pd.errors.EmptyDataError:
                print(f"  Warning: CSV file '{base_filename_with_ext}' is empty. Skipping SQL generation.")
                with open(output_sql_file_path, 'w', encoding='utf-8') as file: file.write(f"-- CSV file '{base_filename_with_ext}' was empty. No SQL generated.\n")
                total_files_processed_successfully +=1
                continue
            except Exception as e:
                print(f"  Error reading CSV file '{base_filename_with_ext}': {e}. Skipping.")
                with open(output_sql_file_path, 'w', encoding='utf-8') as file: file.write(f"-- Error reading CSV file '{base_filename_with_ext}': {e}. No SQL generated.\n")
                continue
            
            if df is None : # Should not happen if read_csv succeeds, but defensive.
                print(f"  Error: DataFrame for '{base_filename_with_ext}' was not loaded. Skipping.")
                with open(output_sql_file_path, 'w', encoding='utf-8') as file: file.write(f"-- DataFrame for '{base_filename_with_ext}' was not loaded. No SQL generated.\n")
                continue

            try:
                if df.empty:
                    print(f"  Warning: CSV '{base_filename_with_ext}' (table '{current_table_name}') has headers but no data. Skipping SQL generation.")
                    with open(output_sql_file_path, 'w', encoding='utf-8') as file: file.write(f"-- CSV '{base_filename_with_ext}' (table '{current_table_name}') had no data rows. No SQL generated.\n")
                    total_files_processed_successfully +=1
                    continue

                # Standardize CSV column names (uppercase, strip whitespace)
                original_csv_columns_standardized = [str(col).strip().upper() for col in df.columns]
                df.columns = original_csv_columns_standardized

                # Identify columns that are in both the CSV and the DB table schema
                # These are the only columns that will be processed and used in SQL
                columns_to_use_in_sql = [col for col in original_csv_columns_standardized if col in all_db_columns_for_current_table]

                if not columns_to_use_in_sql:
                    print(f"  Warning: No common columns found between CSV '{base_filename_with_ext}' and DB table '{current_table_name}'. Skipping SQL generation.")
                    with open(output_sql_file_path, 'w', encoding='utf-8') as file:
                        file.write(f"-- Skipped: No common columns between CSV and DB table '{current_table_name}'.\n")
                    continue
                
                print(f"    Common columns to be used for SQL: {columns_to_use_in_sql}")

                # Create a DataFrame subset containing only the columns to be used in SQL
                df_processed = df[columns_to_use_in_sql].copy()

                # Perform date conversion only on relevant columns within df_processed
                for col_name_for_sql in columns_to_use_in_sql: 
                    if col_name_for_sql in date_type_db_columns_for_current_table:
                        try:
                            df_processed[col_name_for_sql] = pd.to_datetime(df_processed[col_name_for_sql], errors='coerce') \
                                                             .apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)
                            print(f"      Formatted column '{col_name_for_sql}' as datetime.")
                        except Exception as e:
                            print(f"      Warning: Could not format column '{col_name_for_sql}' as datetime: {e}.")
                
                # Apply the safe SQL string converter to all columns in df_processed
                for col_to_convert in df_processed.columns: # these are the columns_to_use_in_sql
                    df_processed[col_to_convert] = df_processed[col_to_convert].apply(safe_sql_string_converter)
                
                with open(output_sql_file_path, 'w', encoding='utf-8') as file:
                    file.write(f"-- SQL for table: {current_table_name} from CSV: {base_filename_with_ext}\n")
                    file.write(f"-- Using columns: {columns_to_use_in_sql}\n")
                    file.write("BEGIN TRY\n")
                    file.write("    BEGIN TRANSACTION;\n\n")
                    
                    pk_col_upper = primary_key_column.upper()

                    for index, row in df_processed.iterrows():
                        sql_query = ""
                        if operation_mode.upper() == 'INSERT':
                            # Use only common columns for INSERT
                            columns_for_insert_sql = ", ".join([f"[{col}]" for col in columns_to_use_in_sql])
                            values_for_insert_sql = ", ".join([row[col] for col in columns_to_use_in_sql])
                            sql_query = f"    INSERT INTO [{current_table_name}] ({columns_for_insert_sql})\n"
                            sql_query += f"    VALUES ({values_for_insert_sql});\n\n"
                        elif operation_mode.upper() == 'UPDATE':
                            # Ensure PK is one of the common columns if we are to use it
                            if pk_col_upper not in columns_to_use_in_sql:
                                error_msg = f"    -- ERROR: Primary Key '{primary_key_column}' (as '{pk_col_upper}') is not in the common columns for CSV '{base_filename_with_ext}' and DB table '{current_table_name}'. Row (CSV line {index + 2}) skipped for UPDATE.\n\n"
                                file.write(error_msg)
                                if index == 0: print(f"    Critical Error for UPDATE: PK '{pk_col_upper}' not in common columns list: {columns_to_use_in_sql}.")
                                continue
                            
                            # Create SET clauses only from common columns, excluding the PK itself
                            set_clauses = [f"[{col_name}] = {row[col_name]}" for col_name in columns_to_use_in_sql if col_name != pk_col_upper]
                            
                            if not set_clauses:
                                file.write(f"    -- INFO: No columns to update for row (CSV line {index + 2}) in '{base_filename_with_ext}' (table '{current_table_name}') besides PK, or PK is the only common column. Skipping UPDATE.\n\n")
                                continue

                            pk_value_formatted = row[pk_col_upper] # PK value from the processed row
                            sql_query = f"    UPDATE [{current_table_name}]\n"
                            sql_query += "    SET " + ",\n        ".join(set_clauses) + "\n"
                            sql_query += f"    WHERE [{pk_col_upper}] = {pk_value_formatted};\n\n"
                        else: 
                            print(f"Critical Error: Invalid operation_mode '{operation_mode}'. Halting for this file.")
                            file.write(f"-- ERROR: Invalid operation_mode. SQL generation stopped.\n")
                            break 
                        file.write(sql_query)
                    
                    if not (operation_mode.upper() != 'INSERT' and operation_mode.upper() != 'UPDATE'):
                        file.write("    COMMIT TRANSACTION;\n")
                        file.write("END TRY\n")
                        file.write("BEGIN CATCH\n")
                        file.write("    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION;\n")
                        file.write("    PRINT 'Error occurred in SQL Execution for table " + current_table_name + ": ' + ERROR_MESSAGE();\n")
                        file.write("    THROW;\n")
                        file.write("END CATCH;\n")
                
                print(f"    Successfully generated SQL script: '{output_sql_file_path}'")
                total_files_processed_successfully +=1
            except Exception as e:
                print(f"  An unexpected error occurred while processing data from '{base_filename_with_ext}' for table '{current_table_name}': {e}")
                with open(output_sql_file_path, 'w', encoding='utf-8') as file: file.write(f"-- Unexpected error processing data from '{base_filename_with_ext}' for table '{current_table_name}': {e}\n")

    except pyodbc.Error as ex: 
        sqlstate = ex.args[0]
        print(f"Fatal Database Connection Error: {sqlstate}. Cannot proceed.")
        print("Please check database server, database name, and authentication details.")
    except Exception as e: 
        print(f"An unexpected error occurred during script setup or DB connection: {e}")
    finally:
        if db_conn:
            db_conn.close()
            print("-" * 30)
            print("Database connection closed.")
    
    print(f"\n--- Processing Complete ---")
    print(f"Total CSV files found: {len(csv_files)}")
    print(f"Total SQL files successfully generated or placeholders created: {total_files_processed_successfully}")

# --- SCRIPT EXECUTION ---
if __name__ == '__main__':
    if operation_mode.upper() not in ['INSERT', 'UPDATE']:
        print(f"Error: Invalid operation_mode '{operation_mode}' in configuration. Script will not run.")
    else:
        process_csv_files()