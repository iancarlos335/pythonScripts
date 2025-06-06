import pandas as pd
import pyodbc
import os
import glob

# --- CONFIGURATION ---
# DATABASE CONNECTION DETAILS
db_server = 'YOUR_SERVER_NAME'      # Replace with your SQL Server name
db_database = 'YOUR_DATABASE_NAME'  # Replace with your database name
# table_name will be derived from the CSV filename

# FOLDER CONFIGURATION
input_csv_folder = 'input_csvs'     # Folder containing your source .csv files
output_sql_folder = 'output_sqls'   # Folder where generated .sql files will be saved

# --- OPERATION MODE ---
# Set to 'INSERT' to generate INSERT statements
# Set to 'UPDATE' to generate UPDATE statements
operation_mode = 'INSERT'  # <<< CHANGE THIS TO 'INSERT' or 'UPDATE' as needed

# --- FOR UPDATE OPERATIONS ONLY ---
# Specify the primary key column name used in the WHERE clause for UPDATEs.
# This column *must* exist in your CSV files AND in the DB table for UPDATE operations.
primary_key_column = 'ID' # <<< CHANGE THIS to your actual primary key column name


# --- FUNCTION TO GET SCHEMA INFORMATION FOR A SPECIFIC TABLE ---
def get_table_schema_info(table_name_param, db_conn):
    """
    Queries the database for all columns and their data types using sys tables.
    Returns a tuple: (
        list_of_all_uppercase_column_names, 
        list_of_uppercase_date_type_column_names,
        list_of_uppercase_timestamp_rowversion_column_names,
        list_of_uppercase_numeric_type_column_names,
        boolean_has_identity_column
    ).
    """
    all_db_columns = []
    date_db_columns = []
    timestamp_db_columns = []
    numeric_db_columns = []
    has_identity_column = False
    cursor = None 
    try:
        cursor = db_conn.cursor()
        # This query joins system tables to get column name, data type, and identity property
        schema_query = f"""
        SELECT 
            c.name AS ColumnName,
            t.name AS DataTypeName,
            c.is_identity
        FROM sys.columns c
        INNER JOIN sys.tables tbl ON tbl.object_id = c.object_id
        INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
        WHERE tbl.name = '{table_name_param}'
        ORDER BY c.column_id;
        """
        cursor.execute(schema_query)
        schema_details = cursor.fetchall()
        
        date_type_list = ['date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset']
        timestamp_type_list = ['timestamp', 'rowversion']
        numeric_type_list = ['decimal', 'numeric', 'float', 'real', 'money', 'smallmoney', 'int', 'bigint', 'smallint', 'tinyint', 'bit']
        
        for detail in schema_details:
            col_name_upper = detail.ColumnName.upper()
            data_type_lower = detail.DataTypeName.lower()

            all_db_columns.append(col_name_upper)
            
            if data_type_lower in date_type_list:
                date_db_columns.append(col_name_upper)
            elif data_type_lower in timestamp_type_list:
                timestamp_db_columns.append(col_name_upper)
            elif data_type_lower in numeric_type_list:
                numeric_db_columns.append(col_name_upper)
            
            if detail.is_identity:
                has_identity_column = True
            
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"    Warning: Database query error while fetching schema for table '{table_name_param}': {sqlstate}.")
    except Exception as e:
        print(f"    An unexpected error occurred while fetching schema for table '{table_name_param}': {e}")
    finally:
        if cursor: cursor.close()
    return all_db_columns, date_db_columns, timestamp_db_columns, numeric_db_columns, has_identity_column


# --- HELPER FUNCTION FOR TYPE-AWARE SQL VALUE FORMATTING ---
def format_sql_value(value, is_numeric):
    """
    Formats a Python value into an SQL literal.
    """
    if pd.isna(value) or value is None:
        return 'NULL'
    
    # For numeric types, just convert to string. Assumes comma-to-dot replacement has already happened.
    if is_numeric:
        # Also handle empty strings in numeric columns as NULL
        str_val = str(value).strip()
        return str_val if str_val else 'NULL'
    
    # For non-numeric types (strings, formatted dates), escape single quotes and wrap in quotes.
    escaped_string = str(value).replace("'", "''")
    return f"'{escaped_string}'"


# --- MAIN PROCESSING ---
def process_csv_files():
    if not os.path.exists(output_sql_folder):
        try:
            os.makedirs(output_sql_folder)
            print(f"Created output folder: {output_sql_folder}")
        except OSError as e:
            print(f"Error: Could not create output folder '{output_sql_folder}': {e}")
            return

    csv_files = glob.glob(os.path.join(input_csv_folder, '*.csv'))
    if not csv_files:
        print(f"No CSV files found in '{input_csv_folder}'.")
        return

    print(f"Found {len(csv_files)} CSV files to process.")
    total_files_processed_successfully = 0
    db_conn = None
    try:
        print("Attempting to connect to the database...")
        db_conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server};DATABASE={db_database};Trusted_Connection=yes')
        print("Database connection successful.")
        print("-" * 30)

        for csv_file_path in csv_files:
            base_filename = os.path.basename(csv_file_path)
            current_table_name = os.path.splitext(base_filename)[0]
            sql_filename = current_table_name + '.sql'
            output_sql_file_path = os.path.join(output_sql_folder, sql_filename)

            all_db_cols, date_db_cols, ts_db_cols, numeric_db_cols, has_identity = get_table_schema_info(current_table_name, db_conn)

            if not all_db_cols:
                print(f"  Skipping CSV '{base_filename}' as no schema was retrieved for table '{current_table_name}'.")
                with open(output_sql_file_path, 'w', encoding='utf-8') as file:
                    file.write(f"-- Skipped: No database schema found for table '{current_table_name}'.\n")
                continue 

            try:
                df = pd.read_csv(csv_file_path, dtype=str, delimiter=';')
            except Exception as e:
                print(f"  Error reading CSV file '{base_filename}': {e}. Skipping.")
                with open(output_sql_file_path, 'w', encoding='utf-8') as file:
                    file.write(f"-- Error reading CSV file '{base_filename}': {e}. No SQL generated.\n")
                continue

            try:
                if df.empty:
                    print(f"  Warning: CSV '{base_filename}' has headers but no data. Skipping SQL generation.")
                    with open(output_sql_file_path, 'w', encoding='utf-8') as file:
                        file.write(f"-- CSV '{base_filename}' had no data rows. No SQL generated.\n")
                    total_files_processed_successfully +=1
                    continue

                csv_cols_standardized = [str(col).strip().upper() for col in df.columns]
                df.columns = csv_cols_standardized

                common_cols_before_ts_filter = [col for col in csv_cols_standardized if col in all_db_cols]
                columns_to_use_in_sql = [col for col in common_cols_before_ts_filter if col not in ts_db_cols]
                excluded_ts_cols = [col for col in common_cols_before_ts_filter if col in ts_db_cols]
                
                if not columns_to_use_in_sql:
                    print(f"  Warning: No usable columns found for '{base_filename}'. Skipping SQL generation.")
                    with open(output_sql_file_path, 'w', encoding='utf-8') as file:
                        file.write(f"-- Skipped: No common/usable columns between CSV and DB table '{current_table_name}'.\n")
                    continue
                
                if excluded_ts_cols:
                    print(f"    Excluding Timestamp/Rowversion columns: {excluded_ts_cols}")

                df_processed = df[columns_to_use_in_sql].copy()

                for col in columns_to_use_in_sql:
                    if col in date_db_cols:
                        df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce').apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)
                        print(f"      - Formatted date column: '{col}'")
                    elif col in numeric_db_cols:
                        df_processed[col] = df_processed[col].str.replace(',', '.', regex=False)

                with open(output_sql_file_path, 'w', encoding='utf-8') as file:
                    file.write(f"-- SQL for table: {current_table_name} from CSV: {base_filename}\n")
                    if excluded_ts_cols: file.write(f"-- Excluded Timestamp/Rowversion columns: {excluded_ts_cols}\n")
                    
                    file.write("BEGIN TRY\n")
                    file.write("    BEGIN TRANSACTION;\n\n")

                    # Conditionally add SET IDENTITY_INSERT ON
                    if operation_mode.upper() == 'INSERT' and has_identity:
                        file.write(f"    SET IDENTITY_INSERT [{current_table_name}] ON;\n\n")
                    
                    pk_col_upper = primary_key_column.upper()

                    for index, row in df_processed.iterrows():
                        if operation_mode.upper() == 'INSERT':
                            values_for_insert = [format_sql_value(row[col], col in numeric_db_cols) for col in columns_to_use_in_sql]
                            columns_for_insert_sql = ", ".join([f"[{col}]" for col in columns_to_use_in_sql])
                            sql_query = f"    INSERT INTO [{current_table_name}] ({columns_for_insert_sql})\n"
                            sql_query += f"        VALUES ({', '.join(values_for_insert)});\n\n"
                        elif operation_mode.upper() == 'UPDATE':
                            if pk_col_upper not in columns_to_use_in_sql:
                                if index == 0: print(f"    Critical Error for UPDATE: PK '{pk_col_upper}' not in usable columns list.")
                                file.write(f"    -- ERROR: Primary Key '{pk_col_upper}' not in usable columns list for row (CSV line {index + 2}). Skipped.\n\n")
                                continue
                            
                            set_clauses = [f"[{col}] = {format_sql_value(row[col], col in numeric_db_cols)}" for col in columns_to_use_in_sql if col != pk_col_upper]
                            
                            if not set_clauses:
                                file.write(f"    -- INFO: No columns to update for row (CSV line {index + 2}). Skipping.\n\n")
                                continue

                            pk_value_formatted = format_sql_value(row[pk_col_upper], pk_col_upper in numeric_db_cols)
                            sql_query = f"    UPDATE [{current_table_name}]\n"
                            sql_query += "    SET " + ",\n        ".join(set_clauses) + "\n"
                            sql_query += f"    WHERE [{pk_col_upper}] = {pk_value_formatted};\n\n"
                        else: 
                            file.write("-- ERROR: Invalid operation_mode. SQL generation stopped.\n")
                            break 
                        file.write(sql_query)
                    
                    # Conditionally add SET IDENTITY_INSERT OFF
                    if operation_mode.upper() == 'INSERT' and has_identity:
                        file.write(f"    SET IDENTITY_INSERT [{current_table_name}] OFF;\n\n")

                    if operation_mode.upper() in ['INSERT', 'UPDATE']:
                        file.write("    COMMIT TRANSACTION;\n")
                        file.write("END TRY\n")
                        file.write("BEGIN CATCH\n")
                        file.write("    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION;\n")
                        file.write("    PRINT 'Error occurred in SQL Execution for table " + current_table_name + ": ' + ERROR_MESSAGE();\n")
                        file.write("    THROW;\n")
                        file.write("END CATCH;\n")
                
                total_files_processed_successfully +=1
            except Exception as e:
                print(f"  An unexpected error occurred while processing data from '{base_filename}': {e}")
                with open(output_sql_file_path, 'w', encoding='utf-8') as file: 
                    file.write(f"-- Unexpected error processing data from '{base_filename}': {e}\n")

    except pyodbc.Error as ex: 
        print(f"Fatal Database Connection Error: {ex.args[0]}. Cannot proceed.")
    except Exception as e: 
        print(f"An unexpected error occurred during script setup or DB connection: {e}")
    finally:
        if db_conn:
            db_conn.close()
            print("-" * 30)
            print("Database connection closed.")
    
    print(f"\n--- Processing Complete ---")
    print(f"Total SQL files generated or placeholders created: {total_files_processed_successfully}")

# --- SCRIPT EXECUTION ---
if __name__ == '__main__':
    if operation_mode.upper() not in ['INSERT', 'UPDATE']:
        print(f"Error: Invalid operation_mode '{operation_mode}'. Script will not run.")
    else:
        process_csv_files()