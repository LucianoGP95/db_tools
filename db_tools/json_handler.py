import os, re, json, sqlite3
from db_tools import SQLite_Handler

class JSONhandler(SQLite_Handler):
    def __init__(self, db_name, rel_path=None):
        super().__init__(db_name, rel_path)  #Calls the parent class constructor
        """Initialize the JSONhandler with the database connection."""
        # Call the parent class constructor (Database's __init__)
        super().__init__(db_name, rel_path)

    def _sanitize_table_name(self, table_name):
        """Sanitize the table name to ensure it follows SQLite's naming rules."""
        # Replace special characters with underscores
        table_name = re.sub(r'\W', '_', table_name)
        # If the name starts with a number, prefix it with an underscore
        if table_name[0].isdigit():
            table_name = f"_{table_name}"
        return table_name

    def _create_table_dynamic(self, table_name, metadata):
        table_name = self._sanitize_table_name(table_name)
        try:
            # Ensure table exists
            self.cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" (filename TEXT);')

            # Get existing columns
            self.cursor.execute(f'PRAGMA table_info("{table_name}")')
            existing_columns = {row[1] for row in self.cursor.fetchall()}

            # Add missing columns
            for key, value in metadata.items():
                if key not in existing_columns:
                    if isinstance(value, int):
                        dtype = "INTEGER"
                    elif isinstance(value, float):
                        dtype = "REAL"
                    else:
                        dtype = "TEXT"
                    try:
                        self.cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{key}" {dtype};')
                    except sqlite3.Error as e:
                        print(f"Failed to add column {key}: {e}")

            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error updating table {table_name}: {e}")

    def _insert_metadata_dynamic(self, table_name, metadata):
        table_name = self._sanitize_table_name(table_name)
        # Convert lists/dicts to strings
        for key in metadata:
            if isinstance(metadata[key], (list, dict)):
                metadata[key] = json.dumps(metadata[key], ensure_ascii=False)

        keys = list(metadata.keys())
        placeholders = ", ".join(["?" for _ in keys])
        column_names = ", ".join([f'"{k}"' for k in keys])
        values = [metadata[k] for k in keys]

        try:
            self.cursor.execute(
                f'INSERT OR IGNORE INTO "{table_name}" ({column_names}) VALUES ({placeholders});',
                values
            )
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error inserting into table {table_name}: {e}")

    def process_jsons(self, folder_path):
            """Process all JSON files in a directory and insert their metadata into the database."""
            for root, _, files in os.walk(folder_path):
                for file in files:
                    if file.endswith(".json"):
                        json_path = os.path.join(root, file)
                        parent_folder = os.path.basename(root)  # Extract the parent folder as the table name
                        
                        # Read and insert the metadata from the JSON file
                        with open(json_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)

                        # Use all data as metadata
                        data["filename"] = file.split(".")[0]
                        if isinstance(data.get("tags"), list):
                            data["tags"] = ";".join(data["tags"])

                        self._create_table_dynamic(parent_folder, data)
                        self._insert_metadata_dynamic(parent_folder, data)
                        
                        # Optionally, delete the JSON file after processing it
                        os.remove(json_path)
