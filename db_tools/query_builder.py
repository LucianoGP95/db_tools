import sqlite3

class QueryBuilder():
    def __init__(self, data: dict):
        self.table_name = data.get("table_name")
        self.columns = data.get("columns")
        self.columns_types = data.get("column_types")
        self.foreign_key = data.get("foreign_key")

    def create_table(self):
        '''Uses the CREATE command to build a table'''
        if not self.check_requirements("table_name", self.table_name, str):
            return
        if not self.check_requirements("columns", self.columns, list):
            return
        if not self.columns_types:
            column_defs = [f"{col} TEXT" for col in self.columns]  # Defaults to type TEXT
        else:
            column_defs = [f"{col} {col_type}" for col, col_type in zip(self.columns, self.columns_types)] # Dynamic type

        if self.foreign_key:
            column_defs.append(
                f"FOREIGN KEY({self.foreign_key}) REFERENCES {self.foreign_key}_table(id)"
            )

        columns_str = ", ".join(column_defs)
        return f"CREATE TABLE {self.table_name} ({columns_str});"

    def check_requirements(self, name: str, value, expected_type=None):
        '''Checks if a value exists and matches an expected type'''
        if value is None:
            print(f"Missing required field: {name}")
            return False
        if expected_type and not isinstance(value, expected_type):
            print(f"Field '{name}' must be of type {expected_type.__name__}, got {type(value).__name__}")
            return False
        return True

if __name__ == "__main__":
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Define test data
    data = {
        "table_name": "test_table",
        "columns": ["id", "name", "email"],
        "foreign_key": "name",
        "column_types": ["INTEGER PRIMARY KEY", "TEXT", "TEXT"] # or set to 'id' to test FK
    }

    # Build and execute query
    qb = QueryBuilder(data)
    create_query = qb.create_table()

    if create_query:
        print("Executing SQL:", create_query)
        cursor.execute(create_query)
        # Show table schema
        cursor.execute("PRAGMA table_info(test_table);")
        rows = cursor.fetchall()
        print("\nTable schema:")
        for row in rows:
            print(row)

    conn.close()
