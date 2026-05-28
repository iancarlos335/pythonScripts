# Gemini AI Instructions & Repository Context

This document serves as the persistent system-level memory, guidance instructions, and onboarding guide for Gemini and other advanced AI coding assistants operating on the `pythonScripts` repository.

---

## 🧠 Core System Memories & Environment Constants

When interacting with this repository, always remember the following environment invariants:
- **Operating System**: Windows
- **Database Engine**: Microsoft SQL Server
- **Default Authentication Scheme**: **Windows Authentication** (`Trusted_Connection=yes`). Do not replace default connection parameters with UID/PWD flows unless explicitly requested by the user.
- **Dependency Pipeline**: Scripts leverage `pandas` for intermediate data parsing, `pyodbc` for low-level database connection routing, and standard regular expressions for parsing text catalogs.

---

## 🗺️ Codebase Index for AI Agents

Here is a map of the repository's files to guide your context gathering:

| Script Path | Primary Role | Core Dependencies | Key Configuration Parameters |
| :--- | :--- | :--- | :--- |
| [`tables_modifications.py`](file:///C:/Barao/Projetos/pythonScripts/tables_modifications.py) | Dynamic table data sync with pre-delete & update/insert engines. | `pandas`, `pyodbc` | `source_where_column`, `source_where_value`, `source_where_negate`, `execute_pre_delete_on_target`, `operation_mode`, `primary_key_column` |
| [`db_sync.py`](file:///C:/Barao/Projetos/pythonScripts/db_sync.py) | Replicates tables, views, procedures, UDFs, and triggers. | `pyodbc`, `sys` | `SOURCE_CONN_STR`, `DEST_CONN_STR` |
| [`strings_finder.py`](file:///C:/Barao/Projetos/pythonScripts/strings_finder.py) | CLI text analyzer for unique/repeated substrings. | `argparse`, `collections` | Mode argument (`repeated` \| `unique`) |
| [`cnpj_unmasked.py`](file:///C:/Barao/Projetos/pythonScripts/cnpj_unmasked.py) | Punctuation cleaning filter for CNPJ lists. | `re` | `file_path` |

---

## 🛠️ Key Task Execution Workflows

### 1. Modifying & Testing `tables_modifications.py`
If the user asks you to modify table synchronization, pay close attention to structural validation:
- **Testing Compilation**: Run `python -m py_compile tables_modifications.py` to assert syntactic correctness.
- **Handling Data Filter Negations**:
  - The `source_where_negate` boolean controls whether queries use `=` (equals) or `<>` (not equals).
  - Both data fetching (`fetch_data_for_table`) and pre-deletion pass run this dynamic operator logic:
    ```python
    operator = '<>' if negate else '='
    ```
  - Ensure any structural changes to SQL queries maintain this parameterized, SQL-injection-safe syntax.

### 2. Extending `db_sync.py`
When generating database objects:
- Remember that SQL Server requires correct type-conversion rules (e.g., handling length bounds for `VARCHAR` vs `NVARCHAR` vs `VARBINARY` and correctly checking `is_identity` to write the standard SQL Server PK constraint).
- Keep scripting objects robust by using `sp_helptext` to extract definitions.

### 3. CLI Pattern Matching with `strings_finder.py`
- Command-line parameters are parsed strictly via `argparse`. Ensure additions are properly registered as arguments/choices in `main()`.

---

## 📢 AI Behavior Guidelines
- **Maintain Comments**: Preserve the user's header remarks, inline warnings, and database setup commentary unless explicitly requested to rebuild them.
- **Safety First**: Never hardcode high-privilege passwords or keys into connection string templates in code files.
- **Keep it Simple**: Prioritize performance and safety by using standard database connection isolation and structured pandas data handling.
