import sqlite3
from sqlite_handler import SQLite_Handler

class QueryBuilder(SQLite_Handler):
    def __init__(self, db_name, db_folder_path=None, rel_path=False):
        super().__init__(db_name, db_folder_path, rel_path)  #Calls the parent class constructor

    def create_table(self, data: dict, foreign_key: list) -> str:
        '''Uses the CREATE command to build a table from a json'''
        self.table_name = data.get("table_name")
        self.columns = data.get("columns")
        self.columns_types = data.get("column_types")
        if foreign_key:
            self.foreign_key = [foreign_key] if isinstance(foreign_key, str) else foreign_key
        else:
            fk_data = data.get("foreign_key")
            self.foreign_key = [fk_data] if isinstance(fk_data, str) else fk_data
        if not self._check_requirements("table_name", self.table_name, str):
            return
        if not self._check_requirements("columns", self.columns, list):
            return
        if not self.columns_types:
            column_defs = [f"{col} TEXT" for col in self.columns]  # Defaults to type TEXT
        else:
            column_defs = [f"{col} {col_type}" for col, col_type in zip(self.columns, self.columns_types)] # Dynamic type
        for key in self.foreign_key:
            ref_table = key.rstrip("_id") + "s"  # crude plural logic
            column_defs.append(f"FOREIGN KEY({key}) REFERENCES {ref_table}(id)")
        columns_str = ", ".join(column_defs)
        return f"CREATE TABLE {self.table_name} ({columns_str});"

    def migrate_table(self, table_name: str, foreign_key: str=None, verbose: bool=True):
        '''Creates a copy of a table and deletes it, allowing for foreign keys set'''
        try:
            # Create temporal name
            temp_name = f"{table_name}_old"
            # Rename source table to temporal name
            self.cursor.execute(f"ALTER TABLE {table_name} RENAME TO {temp_name};")
            # Get source table info
            self.cursor.execute(f"PRAGMA table_info({temp_name});")
            # Get source columns
            old_columns = [row[1] for row in self.cursor.fetchall()]
            # Get query with old columns
            data = {
                "table_name": table_name,
                "columns": old_columns,
                "foreign_key": foreign_key
            }
            query_builder = QueryBuilder(data)
            query = query_builder.create_table()
            # Create new table
            self.cursor.execute(query)
            # Get target table info
            self.cursor.execute(f"PRAGMA table_info({table_name});")
            # Get new columns info
            new_columns = [row[1] for row in self.cursor.fetchall()]
            # Prepare columns
            common_columns = list(set(old_columns) & set(new_columns))
            columns_str = ", ".join(common_columns)
            # Insert columns
            self.cursor.execute(f"INSERT INTO {table_name} ({columns_str}) SELECT {columns_str} FROM {temp_name};")
            # Remove old table
            self.cursor.execute(f"DROP TABLE {temp_name};")
            self.conn.commit()
            print(f"Foreign key set as *{foreign_key}*") if isinstance(foreign_key, str) and verbose else None
            print(f"Table *{table_name}* migrated successfully.") if verbose else None
        except Exception as e:
            raise Exception(f"Error while migrating table: {str(e)}")

    def filter_rows_by_tags(self,
        table_name: str,
        column_name: str,
        whitelist: list = None,
        blacklist: list = None
    ) -> list:
        """Query a table with a comma-separated column, combining multiple inclusion/exclusion conditions.
        Args:
            table_name: Name of the table to query
            column_name: Column containing comma-separated values
            required_values: List of values that MUST be present
            excluded_values: List of values that MUST NOT be present
        Returns:
            List of matching rows"""
        conditions = []
        params = []
        # Add required values (AND conditions)
        if whitelist:
            for val in whitelist:
                conditions.extend([
                    f"({column_name} = ?)",
                    f"({column_name} LIKE ?)",
                    f"({column_name} LIKE ?)",
                    f"({column_name} LIKE ?)"
                ])
                params += [val, f"{val},%", f"%,{val}", f"%,{val},%"]
        # Add excluded values (AND NOT conditions)
        if blacklist:
            for val in blacklist:
                conditions.extend([
                    f"({column_name} != ?)",
                    f"({column_name} NOT LIKE ?)",
                    f"({column_name} NOT LIKE ?)",
                    f"({column_name} NOT LIKE ?)"
                ])
                params += [val, f"{val},%", f"%,{val}", f"%,{val},%"]
        # Build final query
        query = f"SELECT * FROM {table_name}"
        if conditions:
            query += " WHERE " + " AND ".join([f"({' OR '.join(conditions[i:i+4])})" 
                    for i in range(0, len(conditions), 4)])
        # Execute safely with parameter substitution
        self.cursor.execute(query, params)
        rows = self.cursor.fetchall()
        return rows

    def _check_requirements(self, name: str, value, expected_type=None) -> bool:
        '''Checks if a value exists and matches an expected type'''
        if value is None:
            print(f"❌ Missing required field: {name}")
            return False
        if expected_type and not isinstance(value, expected_type):
            print(f"❌ Field '{name}' must be of type {expected_type.__name__}, got {type(value).__name__}")
            return False
        return True

if __name__ == "__main__":
    # Connect in-memory for testing
    qb = QueryBuilder(":memory:")
    
    data = {
        "table_name": "test_table",
        "columns": ["id", "name", "email"],
        "column_types": ["INTEGER PRIMARY KEY", "TEXT", "TEXT"],
        "foreign_key": ["id"]  # assumes reference to another table
    }

    query = qb.create_table(data, foreign_key=["id"])
    if query:
        print("Executing SQL:", query)
        qb.cursor.execute(query)
        qb.cursor.execute("PRAGMA table_info(test_table);")
        for row in qb.cursor.fetchall():
            print(row)

