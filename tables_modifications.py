import pandas as pd
import pyodbc
import os
import glob

# --- CONFIGURATION ---
# SOURCE DATABASE CONNECTION DETAILS (for fetching initial data)
source_db_server = 'YOUR_SOURCE_SERVER_NAME'        # Replace with your source SQL Server name
source_db_database = 'YOUR_SOURCE_DATABASE_NAME'    # Replace with your source database name
source_db_driver = '{ODBC Driver 17 for SQL Server}' # Default ODBC driver
source_db_trusted_connection = True                 # Set to False if using UID/PWD
source_db_uid = 'YOUR_SOURCE_UID'                   # Your username for the source DB
source_db_pwd = 'YOUR_SOURCE_PWD'                   # Your password for the source DB

# TARGET DATABASE CONNECTION DETAILS (for schema lookup and where SQL will be run)
target_db_server = 'YOUR_TARGET_SERVER_NAME'      # Replace with your target SQL Server name
target_db_database = 'YOUR_TARGET_DATABASE_NAME'  # Replace with your target database name
target_db_driver = '{ODBC Driver 17 for SQL Server}' # Default ODBC driver
target_db_trusted_connection = True                 # Set to False if using UID/PWD
target_db_uid = 'YOUR_TARGET_UID'                   # Your username for the target DB
target_db_pwd = 'YOUR_TARGET_PWD'                   # Your password for the target DB
# table_name will be derived from the fetched table names

# FOLDER CONFIGURATION
input_csv_folder = 'input_csvs'     # Folder containing your source .csv files (will be deprecated)
# output_sql_folder has been removed as SQL will be executed directly.
table_list_file = 'tables_to_fetch.txt' # File with list of tables to fetch from source

# --- DATA FETCHING CONFIGURATION ---
source_where_column = 'YOUR_WHERE_COLUMN' # Column to use in the WHERE clause for fetching data from source AND for pre-delete on target
source_where_value = 'SOME_VALUE'         # Value for the WHERE clause (may need to be dynamic)

# --- TARGET TABLE PRE-OPERATION ---
execute_pre_delete_on_target = False # SET TO TRUE to enable deleting rows from target table based on source_where_column/value before inserts/updates

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
    Handles None, NaN, string 'None' (case-insensitive) as SQL NULL.
    Converts Python bool to 1 or 0.
    Handles string 'True'/'False' (case-insensitive) as 1/0 if target is_numeric.
    """
    if pd.isna(value) or value is None:
        return 'NULL'

    # Handle Python bool type explicitly -> convert to 1 or 0
    if isinstance(value, bool):
        return '1' if value else '0'

    if is_numeric: # Target column is numeric (e.g., INT, DECIMAL, BIT)
        str_val = str(value).strip()
        # Ensure that if a numeric value somehow becomes an empty string or 'None' string (case-insensitive), it's NULL
        if not str_val or str_val.lower() == 'none':
            return 'NULL'
        # Handle string representations of booleans if the target is numeric (e.g. BIT column)
        if str_val.lower() == 'true':
            return '1'
        if str_val.lower() == 'false':
            return '0'
        return str_val # Numeric values are not quoted
    
    # For non-numeric types (strings, formatted dates)
    str_value_representation = str(value)
    # If the string representation is 'None' (case-insensitive) treat it as NULL
    if str_value_representation.strip().lower() == 'none':
        return 'NULL'
        
    # Escape single quotes and wrap in SQL single quotes for string literals
    escaped_string = str_value_representation.replace("'", "''")
    return f"'{escaped_string}'"

# --- HELPER FUNCTION TO GET SCALAR VALUE FROM ROW ---
def get_scalar_value_from_row(r, column_name, df_ref):
    """
    Retrieves a scalar value from a row, handling cases where a column name
    might represent multiple actual columns in df_ref (due to duplicates).
    If df_ref[column_name] is a DataFrame, it means column_name was duplicated.
    In this case, r[column_name] will be a Series, so we take the first value.
    Otherwise, r[column_name] is already a scalar.
    """
    val = r[column_name]
    # Check the structure of the DataFrame this row came from
    if isinstance(df_ref[column_name], pd.DataFrame):
        return val.iloc[0]  # val is a Series, take its first element
    return val  # val is already a scalar


# --- MAIN PROCESSING ---
def process_data_and_generate_sql(): # Renamed from process_csv_files
    # --- Fetch all data from the source database ---
    # Note: Add relevant parameters for trusted_conn, uid, pwd if not using trusted connection for source
    fetched_data_map = fetch_all_data_from_source(
        table_list_filepath=table_list_file,
        server=source_db_server,
        database=source_db_database,
        driver=source_db_driver,
        where_column=source_where_column,
        where_value=source_where_value,
        trusted_conn=source_db_trusted_connection,
        uid=source_db_uid,
        pwd=source_db_pwd
    )

    if not fetched_data_map:
        print("No data fetched from source. Exiting SQL execution process.") # Changed "generation" to "execution"
        return

    # Removed output_sql_folder creation logic
    
    print(f"\nStarting SQL operations for {len(fetched_data_map)} tables.")
    tables_attempted_in_delete_pass = 0
    tables_deleted_successfully = 0
    tables_attempted_in_data_pass = 0
    total_tables_committed_successfully = 0
    
    target_db_conn = None

    try:
        print("Attempting to connect to the TARGET database...")
        target_db_conn = create_db_connection(
            server=target_db_server,
            database=target_db_database,
            driver=target_db_driver,
            trusted_connection=target_db_trusted_connection,
            username=target_db_uid,
            password=target_db_pwd
        )
        if not target_db_conn:
            print("Fatal: Could not connect to TARGET database. Cannot proceed.")
            return
        print("Target database connection successful.")
        print("-" * 40)

        # --- PRE-DELETION PASS ---
        if execute_pre_delete_on_target:
            print("\n--- Starting Pre-Deletion Pass ---")
            table_names_for_processing = list(fetched_data_map.keys()) 
            for i, current_table_name in enumerate(table_names_for_processing):
                tables_attempted_in_delete_pass += 1
                print(f"\nPre-Deleting from table ({tables_attempted_in_delete_pass}/{len(table_names_for_processing)}): '{current_table_name}'")
                
                if not source_where_column or source_where_column.strip() == "":
                    print(f"  WARNING: `source_where_column` is not defined or empty. Skipping pre-delete for table '{current_table_name}'.")
                    continue

                cursor = None
                try:
                    cursor = target_db_conn.cursor()
                    # Check if table exists before attempting delete to avoid errors on non-existent tables
                    # (though get_table_schema_info in the data pass would also catch this,
                    #  doing a light check here can make pre-delete more robust if table list is dynamic)
                    # For simplicity now, assume tables exist if they are in fetched_data_map.
                    # A more robust check: cursor.tables(table=current_table_name, tableType='TABLE').fetchone()

                    delete_sql = f"DELETE FROM [{current_table_name}] WHERE [{source_where_column}] = ?;"
                    print(f"    Executing: {delete_sql} (Parameter: '{source_where_value}')")
                    
                    # Execute the delete command
                    cursor.execute(delete_sql, source_where_value)
                    deleted_rows_count = cursor.rowcount
                    
                    # Commit the delete for this table
                    target_db_conn.commit() 
                    
                    print(f"    Successfully deleted {deleted_rows_count if deleted_rows_count != -1 else 'an unconfirmed number of'} rows from '{current_table_name}' and committed changes.")
                    tables_deleted_successfully += 1

                except pyodbc.Error as del_err:
                    error_code = del_err.args[0]
                    error_message = str(del_err)
                    print(f"    DATABASE ERROR during Pre-Delete for table '{current_table_name}' (Code: {error_code}): {error_message}")
                    print(f"      Query attempted: {delete_sql} with param '{source_where_value}'")
                    try:
                        # Rollback in case the error left the transaction in an uncommittable state
                        target_db_conn.rollback()
                        print(f"    Rolled back transaction for table '{current_table_name}' due to pre-delete error.")
                    except pyodbc.Error as rb_err:
                        print(f"      CRITICAL: Failed to ROLLBACK after pre-delete error for table '{current_table_name}': {rb_err}. Connection might be unstable.")
                except Exception as e_del_generic:
                    print(f"    UNEXPECTED NON-DATABASE ERROR during Pre-Delete for table '{current_table_name}': {e_del_generic}")
                    # Non-pyodbc errors might not require a DB rollback unless a transaction was started and not handled by pyodbc layer.
                    # For safety, attempt rollback if connection seems active.
                    if target_db_conn and not target_db_conn.closed : # Check if connection is usable
                        try:
                            target_db_conn.rollback()
                            print(f"    Attempted rollback for table '{current_table_name}' due to unexpected pre-delete error.")
                        except pyodbc.Error as rb_err:
                             print(f"      CRITICAL: Failed to ROLLBACK after unexpected pre-delete error for table '{current_table_name}': {rb_err}. Connection might be unstable.")
                finally:
                    if cursor:
                        cursor.close()
            print("--- Pre-Deletion Pass Complete ---")
            print("-" * 40)
        else:
            print("\nSkipping Pre-Deletion Pass as `execute_pre_delete_on_target` is False.")
            print("-" * 40)

        # --- DATA INSERTION/UPDATE PASS ---
        print("\n--- Starting Data Insertion/Update Pass ---")
        for current_table_name, df in fetched_data_map.items():
            tables_attempted_in_data_pass += 1
            print(f"\nProcessing Data for table ({tables_attempted_in_data_pass}/{len(fetched_data_map)}): '{current_table_name}'")

            all_db_cols, date_db_cols, ts_db_cols, numeric_db_cols, has_identity = get_table_schema_info(current_table_name, target_db_conn)

            if not all_db_cols:
                print(f"  Skipping data operations for table '{current_table_name}' as no schema was retrieved from TARGET database.")
                continue
            
            try: # This try is for data preparation for the current table
                if df.empty:
                    print(f"  Warning: Fetched data for '{current_table_name}' is empty. No data operations to execute.")
                    total_tables_committed_successfully +=1 # Counts as "successful" as no data ops failed
                    continue

                csv_cols_standardized = [str(col).strip().upper() for col in df.columns]
                df.columns = csv_cols_standardized
                seen_columns = set()
                duplicate_columns = set()
                for col_name in df.columns:
                    if col_name in seen_columns: duplicate_columns.add(col_name)
                    else: seen_columns.add(col_name)
                if duplicate_columns:
                    print(f"  Warning: Duplicate column names in fetched data for '{current_table_name}': {list(duplicate_columns)}")

                common_cols_before_ts_filter = [col for col in csv_cols_standardized if col in all_db_cols]
                columns_to_use_in_sql = [col for col in common_cols_before_ts_filter if col not in ts_db_cols]
                excluded_ts_cols = [col for col in common_cols_before_ts_filter if col in ts_db_cols]
                
                if not columns_to_use_in_sql:
                    print(f"  Warning: No usable columns for table '{current_table_name}' after schema matching. Skipping data operations.")
                    continue
                if excluded_ts_cols:
                    print(f"    Excluding Timestamp/Rowversion columns from SQL operations: {excluded_ts_cols}")

                df_processed = df[columns_to_use_in_sql].copy()
                for col in columns_to_use_in_sql: # Data type conversions
                    if col in date_db_cols:
                        # ... (date formatting logic remains identical) ...
                        if isinstance(df_processed[col], pd.DataFrame):
                            # print(f"    Warning: Duplicate column name '{col}' encountered in date processing...")
                            for i in range(df_processed[col].shape[1]):
                                df_processed[col].iloc[:, i] = pd.to_datetime(df_processed[col].iloc[:, i], errors='coerce').apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)
                        elif isinstance(df_processed[col], pd.Series):
                            df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce').apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)
                        # else: print(f"    Warning: Column '{col}' in date_db_cols is neither Series nor DataFrame...")
                        # print(f"      - Formatted date column: '{col}'")
                    elif col in numeric_db_cols:
                        # ... (numeric formatting logic remains identical) ...
                        if isinstance(df_processed[col], pd.DataFrame):
                            # print(f"    Info: Handling multiple instances for numeric column '{col}'...")
                            for i in range(df_processed[col].shape[1]):
                                if not pd.api.types.is_numeric_dtype(df_processed[col].iloc[:, i]):
                                    df_processed[col].iloc[:, i] = df_processed[col].iloc[:, i].astype(str).str.replace(',', '.', regex=False)
                        elif isinstance(df_processed[col], pd.Series):
                            if not pd.api.types.is_numeric_dtype(df_processed[col]):
                                df_processed[col] = df_processed[col].astype(str).str.replace(',', '.', regex=False)
                        # else: print(f"    Warning: Column '{col}' in numeric_db_cols is neither Series nor DataFrame...")
                
                # --- SQL Execution Block for Data Operations (INSERT/UPDATE) ---
                cursor = None
                table_data_ops_successful = False
                try:
                    cursor = target_db_conn.cursor()
                    print(f"    Executing data operations for table '{current_table_name}'. {len(df_processed)} rows to process.")

                    if operation_mode.upper() == 'INSERT' and has_identity:
                        identity_insert_on_sql = f"SET IDENTITY_INSERT [{current_table_name}] ON;"
                        print(f"      Executing: {identity_insert_on_sql}")
                        cursor.execute(identity_insert_on_sql)

                    pk_col_upper = primary_key_column.upper()
                    rows_affected_count = 0
                    processed_row_count_for_table = 0
                    log_every_n_rows = 100 

                    for index, row_data in df_processed.iterrows():
                        processed_row_count_for_table += 1
                        sql_query = ""
                        # ... (INSERT/UPDATE sql_query construction logic remains identical) ...
                        if operation_mode.upper() == 'INSERT':
                            values_for_insert = [format_sql_value(get_scalar_value_from_row(row_data, col, df_processed), col in numeric_db_cols) for col in columns_to_use_in_sql]
                            columns_for_insert_sql = ", ".join([f"[{col}]" for col in columns_to_use_in_sql])
                            sql_query = f"INSERT INTO [{current_table_name}] ({columns_for_insert_sql}) VALUES ({', '.join(values_for_insert)});"
                        elif operation_mode.upper() == 'UPDATE':
                            if pk_col_upper not in columns_to_use_in_sql:
                                if index == 0: print(f"    Critical Error for UPDATE: PK '{pk_col_upper}' not in usable columns. Skipping row.")
                                continue
                            set_clauses = [f"[{col}] = {format_sql_value(get_scalar_value_from_row(row_data, col, df_processed), col in numeric_db_cols)}" for col in columns_to_use_in_sql if col != pk_col_upper]
                            if not set_clauses:
                                print(f"    INFO: No columns to update for row (index {index}). Skipping.")
                                continue
                            pk_value_formatted = format_sql_value(get_scalar_value_from_row(row_data, pk_col_upper, df_processed), pk_col_upper in numeric_db_cols)
                            sql_query = f"UPDATE [{current_table_name}] SET {', '.join(set_clauses)} WHERE [{pk_col_upper}] = {pk_value_formatted};"
                        else:
                            print(f"    ERROR: Invalid operation_mode '{operation_mode}'. Halting.")
                            break
                        
                        if not sql_query:
                            print(f"        Skipping row {index} due to empty query.")
                            continue
                        
                        cursor.execute(sql_query)
                        rows_affected_count += cursor.rowcount if cursor.rowcount != -1 else 1
                        if processed_row_count_for_table % log_every_n_rows == 0:
                            print(f"        ... processed {processed_row_count_for_table} rows ...")

                    if operation_mode.upper() == 'INSERT' and has_identity:
                        identity_insert_off_sql = f"SET IDENTITY_INSERT [{current_table_name}] OFF;"
                        print(f"      Executing: {identity_insert_off_sql}")
                        cursor.execute(identity_insert_off_sql)
                    
                    target_db_conn.commit()
                    print(f"    Successfully committed {rows_affected_count} data operations for table '{current_table_name}'.")
                    table_data_ops_successful = True

                except pyodbc.Error as db_err:
                    print(f"    DATABASE ERROR during data operations for table '{current_table_name}': {db_err.args[0]} - {db_err}")
                    try: target_db_conn.rollback(); print(f"    Rolled back data operations for table '{current_table_name}'.")
                    except pyodbc.Error as rb_err: print(f"      Failed to ROLLBACK data operations: {rb_err}")
                except Exception as e_exec:
                    print(f"    UNEXPECTED ERROR during data operations for table '{current_table_name}': {e_exec}")
                    try: target_db_conn.rollback(); print(f"    Rolled back data operations for table '{current_table_name}'.")
                    except pyodbc.Error as rb_err: print(f"      Failed to ROLLBACK data operations: {rb_err}")
                finally:
                    if cursor: cursor.close()
                    if table_data_ops_successful: total_tables_committed_successfully += 1
                # --- End of SQL Execution Block for Data Operations ---
            
            except Exception as e_prep: 
                print(f"  An unexpected error occurred while preparing data for table '{current_table_name}': {e_prep}")
        print("--- Data Insertion/Update Pass Complete ---")
        print("-" * 40)

    except pyodbc.Error as ex: 
        print(f"Fatal Target Database Connection Error: {ex.args[0]}. Cannot proceed.")
    except Exception as e: 
        print(f"An unexpected error occurred during script setup or Target DB connection: {e}")
    finally:
        if target_db_conn: 
            target_db_conn.close()
            print("Target Database connection closed.")
            print("-" * 40)
    
    print(f"\n--- SQL Execution Process Summary ---")
    if execute_pre_delete_on_target:
        print(f"Pre-Deletion Pass: Attempted on {tables_attempted_in_delete_pass} tables, Successfully deleted from {tables_deleted_successfully} tables.")
    print(f"Data Insertion/Update Pass: Attempted on {tables_attempted_in_data_pass} tables, Successfully committed for {total_tables_committed_successfully} tables.")
    
    # Overall success might be defined differently now.
    # For simplicity, let's say total_tables_committed_successfully refers to the data pass.
    failed_data_tables_count = tables_attempted_in_data_pass - total_tables_committed_successfully
    if failed_data_tables_count > 0:
        print(f"Data Insertion/Update Pass: Failed (rolled back or skipped) for {failed_data_tables_count} tables.")

# --- FUNCTION TO READ TABLE NAMES FROM A TEXT FILE ---
def get_table_names_from_file(filepath="tables_to_fetch.txt"):
    """
    Reads table names from a given text file, one table name per line.
    Returns a list of table names.
    """
    table_names = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                table_name = line.strip()
                if table_name and not table_name.startswith('#'): # Ignore empty lines and comments
                    table_names.append(table_name)
        if not table_names:
            print(f"Warning: No table names found in '{filepath}'.")
        else:
            print(f"Successfully read {len(table_names)} table names from '{filepath}'.")
    except FileNotFoundError:
        print(f"Error: The file '{filepath}' was not found.")
    except Exception as e:
        print(f"An error occurred while reading '{filepath}': {e}")
    return table_names

# --- FUNCTION TO ESTABLISH DATABASE CONNECTION ---
def create_db_connection(server, database, driver, trusted_connection=True, username=None, password=None):
    """
    Establishes a connection to a SQL Server database.
    Returns a pyodbc connection object or None if connection fails.
    """
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};'
    if trusted_connection:
        conn_str += 'Trusted_Connection=yes;'
    else:
        conn_str += f'UID={username};PWD={password};'
    
    try:
        conn = pyodbc.connect(conn_str)
        print(f"Successfully connected to database: {database} on server: {server}")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Error connecting to database: {database} on server: {server}. SQLSTATE: {sqlstate}")
        print(f"Connection string used (sans password): {conn_str.replace(password, '********') if password else conn_str}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during database connection to {server}/{database}: {e}")
        return None

# --- FUNCTION TO FETCH DATA FOR A SINGLE TABLE ---
def fetch_data_for_table(db_conn, table_name, where_column, where_value):
    """
    Fetches data from a specific table based on a WHERE condition.
    Returns a pandas DataFrame or None if an error occurs.
    """
    query = f"SELECT * FROM [{table_name}] WHERE [{where_column}] = ?"
    try:
        print(f"Fetching data from table '{table_name}' with WHERE [{where_column}] = '{where_value}'...")
        # Using parameters for the query is safer (prevents SQL injection)
        df = pd.read_sql_query(query, db_conn, params=[where_value])
        print(f"Successfully fetched {len(df)} rows from '{table_name}'.")
        return df
    except pd.io.sql.DatabaseError as e: # More specific exception for pandas SQL errors
        print(f"Pandas SQL Error while fetching data from table '{table_name}': {e}")
        # Check if the error is due to the table not existing or access issues
        if "Invalid object name" in str(e) or "access" in str(e).lower():
            print(f"  Hint: Check if table '{table_name}' exists and if you have SELECT permissions.")
        return None
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"pyodbc Error while fetching data from table '{table_name}': {sqlstate}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while fetching data from '{table_name}': {e}")
        return None

# --- ORCHESTRATOR FUNCTION FOR DATA FETCHING ---
def fetch_all_data_from_source(
    table_list_filepath, 
    server, 
    database, 
    driver, 
    where_column, 
    where_value, 
    trusted_conn=True, 
    uid=None, 
    pwd=None
):
    """
    Orchestrates the fetching of data for multiple tables from the source database.
    1. Reads table names from the specified file.
    2. Connects to the source database.
    3. For each table, fetches data based on the WHERE condition.
    4. Returns a dictionary of DataFrames {table_name: DataFrame}.
    """
    all_data = {}
    
    table_names = get_table_names_from_file(table_list_filepath)
    if not table_names:
        print("No table names to process. Exiting data fetching.")
        return all_data

    db_conn = create_db_connection(server, database, driver, trusted_connection=trusted_conn, username=uid, password=pwd)
    if not db_conn:
        print("Database connection failed. Cannot fetch data.")
        return all_data

    try:
        for table_name in table_names:
            print(f"\n--- Processing table: {table_name} ---")
            df = fetch_data_for_table(db_conn, table_name, where_column, where_value)
            if df is not None:
                all_data[table_name] = df
            else:
                print(f"  Skipping table '{table_name}' due to previous errors or no data.")
    finally:
        if db_conn:
            print("\nClosing source database connection.")
            db_conn.close()
            
    print(f"\n--- Source Data Fetching Complete ---")
    print(f"Successfully fetched data for {len(all_data)} out of {len(table_names)} tables.")
    return all_data

# --- SCRIPT EXECUTION ---
if __name__ == '__main__':
    if operation_mode.upper() not in ['INSERT', 'UPDATE']:
        print(f"Error: Invalid operation_mode '{operation_mode}'. Script will not run.")
    else:
        process_data_and_generate_sql() # Updated function call