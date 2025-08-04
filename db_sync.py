# db_sync.py
# A single Python script to read database object properties from a source SQL Server
# and replicate them on a destination SQL Server.

import pyodbc
import sys

#
# I. Configuration
# -----------------
# Connection strings for the source and destination databases.
# IMPORTANT: Replace these placeholders with your actual connection strings.
SOURCE_CONN_STR = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=YOUR_SOURCE_SERVER;DATABASE=DATABASE_NAME;Trusted_Connection=yes;"
DEST_CONN_STR = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=YOUR_TARGET_SERVER;DATABASE=DATABASE_NAME;Trusted_Connection=yes;"


#
# II. Core Logic
# --------------
# Functions to handle the synchronization of different database objects.
#

def get_db_connection(conn_str):
    """Establishes and returns a database connection."""
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"ERROR: Connection failed with SQLSTATE {sqlstate}")
        print(ex)
        sys.exit(1)


def execute_on_dest(sql_query, dest_cursor):
    """Executes a given SQL query on the destination database."""
    try:
        dest_cursor.execute(sql_query)
        dest_cursor.commit()
    except pyodbc.Error as ex:
        print(f"ERROR: Failed to execute query on destination: {sql_query[:100]}...")
        print(ex)


def sync_tables(source_cursor, dest_cursor):
    """
    Syncs tables from source to destination.
    It fetches all tables from the source, and for each one that doesn't exist on the destination,
    it generates and executes a CREATE TABLE statement.
    """
    print("Fetching tables from source...")
    source_cursor.execute("SELECT name FROM sys.tables WHERE type = 'U'")
    tables = [row.name for row in source_cursor.fetchall()]

    for table_name in tables:
        # Check if table exists in destination
        dest_cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
        if dest_cursor.fetchone()[0] == 1:
            print(f"Table '{table_name}' already exists in destination. Skipping.")
            continue

        print(f"Creating table '{table_name}' in destination...")

        # Get column definitions from source
        column_sql = """
        SELECT 
            c.name,
            t.name as type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(?)
        ORDER BY c.column_id
        """
        source_cursor.execute(column_sql, table_name)
        columns = source_cursor.fetchall()

        # Generate CREATE TABLE statement
        create_sql = [f"CREATE TABLE [{table_name}] ("]
        column_defs = []
        primary_key_col = None

        for col in columns:
            col_def = f"[{col.name}] {col.type.upper()}"
            
            # Handle data type length/precision
            if col.type in ('varchar', 'nvarchar', 'char', 'nchar', 'varbinary'):
                length = 'MAX' if col.max_length == -1 else col.max_length
                col_def += f"({length})"
            elif col.type in ('decimal', 'numeric'):
                col_def += f"({col.precision}, {col.scale})"
            
            # Handle IDENTITY
            if col.is_identity:
                col_def += " IDENTITY(1,1)"
                primary_key_col = col.name

            # Handle NULL/NOT NULL
            col_def += " NOT NULL" if not col.is_nullable else " NULL"
            column_defs.append(col_def)

        create_sql.append(",\n".join(column_defs))
        
        # Add primary key constraint if an identity column was found
        if primary_key_col:
            create_sql.append(f",\nCONSTRAINT PK_{table_name} PRIMARY KEY CLUSTERED ([{primary_key_col}] ASC)")

        create_sql.append(");")
        
        final_sql = "\n".join(create_sql)
        print(f"Generated SQL for '{table_name}':\n{final_sql}\n")
        
        execute_on_dest(final_sql, dest_cursor)
        print(f"Table '{table_name}' created successfully.")

def sync_scripted_object(object_type, source_cursor, dest_cursor):
    """
    Syncs scripted objects (Views, Procedures, Functions, Triggers) from source to destination.
    It gets the creation script from the source and applies it to the destination.
    """
    
    # Map object types to their system tables and drop syntax
    object_map = {
        'VIEW': {'query': "SELECT name FROM sys.views", 'drop': "DROP VIEW IF EXISTS"},
        'PROCEDURE': {'query': "SELECT name FROM sys.procedures", 'drop': "DROP PROCEDURE IF EXISTS"},
        'FUNCTION': {'query': "SELECT name FROM sys.objects WHERE type_desc LIKE '%FUNCTION'", 'drop': "DROP FUNCTION IF EXISTS"},
        'TRIGGER': {'query': "SELECT name FROM sys.triggers", 'drop': "DROP TRIGGER IF EXISTS"},
    }

    if object_type.upper() not in object_map:
        print(f"ERROR: Unknown object type '{object_type}'.")
        return

    config = object_map[object_type.upper()]
    print(f"Fetching {object_type.lower()}s from source...")
    
    try:
        source_cursor.execute(config['query'])
        objects = [row.name for row in source_cursor.fetchall()]
    except pyodbc.Error as ex:
        print(f"ERROR: Failed to fetch {object_type.lower()}s from source.")
        print(ex)
        return

    for obj_name in objects:
        print(f"Syncing {object_type.lower()} '{obj_name}'...")

        # Get creation script from source
        try:
            source_cursor.execute("sp_helptext ?", obj_name)
            script_rows = source_cursor.fetchall()
            # Some objects might not have a script (e.g., system objects), skip them.
            if not script_rows:
                continue
            create_script = "".join([row.Text for row in script_rows])
        except pyodbc.Error as ex:
            print(f"ERROR: Failed to get script for {obj_name}.")
            print(ex)
            continue

        # Drop the object on the destination if it exists
        drop_sql = f"{config['drop']} [{obj_name}]"
        execute_on_dest(drop_sql, dest_cursor)

        # Recreate the object on the destination
        execute_on_dest(create_script, dest_cursor)
        print(f"{object_type.capitalize()} '{obj_name}' synced successfully.")


#
# III. Main Execution
# -------------------
# The main block that orchestrates the synchronization process.
#

def main():
    """Main function to run the database synchronization."""
    print("Starting database synchronization...")

    # Establish connections
    print("Connecting to source and destination databases...")
    source_conn = get_db_connection(SOURCE_CONN_STR)
    dest_conn = get_db_connection(DEST_CONN_STR)
    source_cursor = source_conn.cursor()
    dest_cursor = dest_conn.cursor()
    print("Connections successful.")

    # Sync objects
    sync_tables(source_cursor, dest_cursor)
    sync_scripted_object('VIEW', source_cursor, dest_cursor)
    sync_scripted_object('PROCEDURE', source_cursor, dest_cursor)
    sync_scripted_object('FUNCTION', source_cursor, dest_cursor)
    sync_scripted_object('TRIGGER', source_cursor, dest_cursor)

    # Clean up
    print("Closing database connections.")
    source_cursor.close()
    source_conn.close()
    dest_cursor.close()
    dest_conn.close()

    print("\nDatabase synchronization complete.")
    print("Please check the destination database and review any error messages above.")

if __name__ == "__main__":
    main()