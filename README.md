# Python Utilities and Database Sync Toolbox

A curated collection of production-grade Python scripts designed for database administration, schema replication, structured data synchronization, text pattern extraction, and CLI-based string analysis. 

---

## 🛠️ Included Tools & Utilities

The toolbox is divided into database engineering tools and high-efficiency text parsing utilities:

### 1. Database Operations

#### 📊 [tables_modifications.py](file:///C:/Barao/Projetos/pythonScripts/tables_modifications.py)
A powerful row-by-row table synchronizer that fetches records from a source database, performs target schema metadata mapping (identifying timestamps, column types, and identity settings), converts types automatically, and securely inserts/updates target databases.
- **Key Features**:
  - Direct database-to-database data transfer (removes need for intermediate CSV files).
  - Dynamic `INSERT` / `UPDATE` operation mode generation.
  - Automatic conversion of Python primitives and Pandas `NaN`/`None` into standard `SQL NULL` parameters.
  - Smart handling of Microsoft SQL Server `IDENTITY_INSERT` states.
  - **Flexible WHERE Filter & Negation**: Filter records based on `source_where_column` and `source_where_value`. Supports conditional negation using `source_where_negate = True` (converts query operators from `=` to `<>`).
  - **Target Pre-deletion**: Delete rows matching/excluding the where filter on the target database prior to executing writes.
  - **FK-Aware Table Ordering**: Automatically reorders the tables listed in `tables_to_fetch.txt` based on foreign key relationships in the target database — parent tables are inserted/updated before their children, and pre-deletion runs in the reverse (child-first) order, avoiding FK constraint violations.

#### 🔄 [db_sync.py](file:///C:/Barao/Projetos/pythonScripts/db_sync.py)
A structural database synchronization script that reads schemas and database objects from a source MS SQL Server and recreates them on a destination database.
- **Key Features**:
  - **Table Schema Sync**: Scans active user tables, queries their catalog parameters, and builds corresponding `CREATE TABLE` scripts (handles precise lengths, precision, scales, nullable flags, identity constraints, and clustered primary keys).
  - **Scripted Object Sync**: Syncs Views, Stored Procedures, User-Defined Functions, and Triggers by fetching structural source code via `sp_helptext`, performing drop guards (`DROP ... IF EXISTS`), and executing them cleanly on the target instance.

### 2. Text Parsing & String Utilities

#### 🔍 [strings_finder.py](file:///C:/Barao/Projetos/pythonScripts/strings_finder.py)
A command-line script to inspect the content of raw text files to determine case-insensitive repeating patterns or highly unique words.
- **Usage**:
  ```bash
  python strings_finder.py <file_path> <mode>
  ```
- **Modes**:
  - `repeated`: Outputs all repeated non-whitespace strings and their frequency, sorted.
  - `unique`: Outputs all non-whitespace strings that appear exactly once.

#### 🪪 [cnpj_unmasked.py](file:///C:/Barao/Projetos/pythonScripts/cnpj_unmasked.py)
A helper script designed to parse, clean, and unmask strings representing Brazilian CNPJ (Cadastro Nacional da Pessoa Jurídica) identifiers, stripping punctuation like `.`, `/`, and `-` using regular expressions.

---

## ⚙️ Prerequisites & Setup

### Database Drivers
These scripts rely on **`pyodbc`** and Microsoft's **ODBC Driver for SQL Server**. Make sure they are installed on your OS:
- **Windows**: [ODBC Driver 17 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) is typically standard or easily downloadable.
- **Python Packages**:
  ```bash
  pip install pandas pyodbc
  ```

> [!IMPORTANT]
> **Authentication Method**: By default, the database synchronization and modification scripts are configured to use **Windows Authentication** (`Trusted_Connection=yes`). Make sure your user environment has appropriate database catalog read/write privileges on both target and source systems.

---

## 🚀 Quick Start & Customization

### Configuring Table Replication (`tables_modifications.py`)

1. Open [tables_modifications.py](file:///C:/Barao/Projetos/pythonScripts/tables_modifications.py) and configure your database target and source connections:
   ```python
   source_db_server = 'SOURCE_SERVER'
   source_db_database = 'SOURCE_DATABASE'
   
   target_db_server = 'TARGET_SERVER'
   target_db_database = 'TARGET_DATABASE'
   ```
2. Define which tables to process in `tables_to_fetch.txt` (one table name per line).
3. Set your execution parameters:
   ```python
   source_where_column = 'FiscalYear'
   source_where_value = '2026'
   source_where_negate = False  # Set to True if you want to sync all years EXCEPT 2026 (<>)
   
   operation_mode = 'INSERT'  # Or 'UPDATE'
   primary_key_column = 'ID'  # Required if operation_mode is 'UPDATE'
   execute_pre_delete_on_target = True  # Clean target records under WHERE condition before inserting
   ```
4. Run the script:
   ```bash
   python tables_modifications.py
   ```

### Synchronizing Schema Objects (`db_sync.py`)

1. Open [db_sync.py](file:///C:/Barao/Projetos/pythonScripts/db_sync.py) and adjust connections:
   ```python
   SOURCE_CONN_STR = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=YOUR_SOURCE_SERVER;DATABASE=DB_NAME;Trusted_Connection=yes;"
   DEST_CONN_STR = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=YOUR_TARGET_SERVER;DATABASE=DB_NAME;Trusted_Connection=yes;"
   ```
2. Run script directly:
   ```bash
   python db_sync.py
   ```

---

## ⚠️ Important Best Practices
> [!WARNING]
> Before executing bulk inserts or pre-deletions on productive databases using `tables_modifications.py`, test the configuration on a development environment first to ensure that column mappings and key assignments align perfectly. Always maintain consistent database backups!
