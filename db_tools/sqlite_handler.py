#V22.0 17/04/2025
import os, json, time, re, sys, shutil, sqlite3
################################################################################

class SQLite_Handler:
    '''SQLite custom handler'''

    def __init__(self, db_name: str, db_folder_path: str = None, rel_path: bool = False):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        # Memory database shortcut
        if db_name == ":memory:":
            self.db_path = ":memory:"
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            print("Test database created in RAM")
            return
        # Check if db_name looks like a valid absolute or relative file path
        if os.path.isabs(db_name) or os.path.sep in db_name:
            self.db_path = os.path.abspath(db_name)
            db_dir = os.path.dirname(self.db_path)
            os.makedirs(db_dir, exist_ok=True)
            # Optionally validate the file extension
            if not self.db_path.lower().endswith(".db"):
                raise ValueError("Database file path must end with '.db'")
            # Proceed to connect
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            print(f"✅ Database loaded from full path: {self.db_path}")
            return
        else:
            # Validate simple db_name
            if not re.match(r'^[\w\-. ]+\.db$', db_name):
                raise ValueError("Database name must be a valid filename ending in '.db'")
            if not rel_path and db_folder_path is None:
                db_dir = os.path.abspath(os.path.join(base_dir, "../database/"))
            elif rel_path and db_folder_path is not None:
                db_dir = os.path.abspath(os.path.join(base_dir, db_folder_path))
            elif db_folder_path is not None:
                db_dir = os.path.abspath(db_folder_path)
            else:
                raise ValueError("Invalid combination of db_folder_path and rel_path.")
        os.makedirs(db_dir, exist_ok=True)
        self.db_path = os.path.join(db_dir, db_name)
        # Connect and log
        if not os.path.exists(self.db_path):
            print(f"✅ Database *{db_name}* created in: {self.db_path}")
        else:
            print(f"✅ Database *{db_name}* found in: {self.db_path}")

        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def get_key_info(self, foreign_keys: bool=False):
        '''Access PRAGMA configrations of the database'''
        foreign_keys = "ON" if foreign_keys == True else "OFF"
        self.cursor.execute(f"PRAGMA foreign_keys = {foreign_keys}")

    def get_table_info(self, table_name: str):
        '''Uses PRAGMA to show table info'''
        self.cursor.execute(f"PRAGMA table_info({table_name});")
        rows = self.cursor.fetchall()
        print("\nTable schema:")
        for row in rows:
            print(row)

    def consult_tables(self, order=None, filter=None, verbose=True):
        '''Shows all the tables in the database. Allows for filtering.'''
        show_order = "name" if order is None else order  # Default order
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' ORDER BY {show_order}")
        if filter:  # First, filters by the full name
            tables = [table[0] for table in cursor.fetchall() if filter.lower() in table[0].lower()]
            if not tables:  # If not successful, filters by initial string
                tables = [table[0] for table in cursor.fetchall() if table[0].lower().startswith(filter.lower())]
        else:
            tables = [table[0] for table in cursor.fetchall()]
        
        _, db_name = os.path.split(self.db_path)
        if verbose:
            print(f"*{db_name}* actual contents:")
            for table in tables:
                print(f"    {table}")
        return tables

    def examine_table(self, table_name: str):
        '''Prints the desired table or tables if given in list or tuple format'''
        table_name = self._input_handler(table_name)
        try:
            cursor = self.conn.cursor()
            for i, table in enumerate(table_name):
                print(f"table {i+1}: {table}")
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                rows = cursor.fetchone()[0]
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                columns_number = len(columns)
                column_names = []  # Preallocation
                for column in columns:  # Get column names
                    column_name = column[1]
                    column_names.append(column_name)
                column_names = tuple(column_names)
                print(f"    Rows: {rows}\n    Columns: {columns_number}")
                cursor.execute(f"SELECT * FROM {table};")
                rows = cursor.fetchall()
                print(f"Columns name: {column_names}")
                for row in rows:  # Gets values row by row
                    print(f"    {row}")
        except Exception as e:
            raise Exception(f"Error while examining tables: {str(e)}")

    def rename_table(self, old_name: str, new_name: str, verbose=True):
        old_name = re.sub(r'\W', '_', old_name)  # To avoid illegal symbols
        new_name = re.sub(r'\W', '_', new_name)
        try:
            self.cursor.execute(f"ALTER TABLE {old_name} RENAME TO {new_name};")
            self.conn.commit()
            print(f"Table *{old_name}* renamed to *{new_name}*") if verbose else None
        except sqlite3.OperationalError as e:
            error_message = str(e)
            if "there is already another table" in error_message:
                print(f"Table *{new_name}* already exists. Skipping renaming.") if verbose else None
            else:
                raise Exception(f"Error while renaming table: {error_message}")

    def rename_column(self, table_name, old_name, new_name, verbose=True):
        try:
            quoted_table_name = f'"{table_name}"'
            quoted_old_name = f'"{old_name}"'
            quoted_new_name = f'"{new_name}"'
            self.cursor.execute(f"ALTER TABLE {quoted_table_name} RENAME COLUMN {quoted_old_name} TO {quoted_new_name};")
            self.conn.commit()
            print(f"Table *{table_name}* renamed from *{old_name}* to *{new_name}*") if verbose else None
        except Exception as e:
            print(f"Error renaming column: {e}")

    def delete_table(self, table_name: str):
        try:
            print(f"Warning: This action will drop the table {table_name}.")
            confirmation = input("Do you want to continue? (y/n): ").strip().lower()
            if confirmation == 'y':
                self.cursor.execute(f"DROP TABLE {table_name};")
                self.conn.commit()
                print(f"{table_name} dropped successfully.")
                print(f"Table *{table_name}* deleted")
                self.consult_tables()
            else:
                print("Operation canceled.")
        except Exception as e:
            raise Exception(f"Error while deleting table: {str(e)}")

    def delete_row(self, row_name: str | list, table_name: str):
        '''Drops row(s) from the desired table'''
        row_name = self._input_handler(row_name)
        try:
            print(f"Warning: This action will drop row(s) from {table_name}.")
            confirmation = input("Do you want to continue? (y/n): ").strip().lower()
            if confirmation == 'y':
                for row in row_name:
                    self.cursor.execute(f"PRAGMA table_info({table_name})")
                    columns_info = self.cursor.fetchall()
                    column_name = columns_info[0][1]
                    self.cursor.execute(f"DELETE FROM {table_name} WHERE {column_name} = '{row}'")
                    self.conn.commit()
                    print(f"{row} dropped successfully.")
                print(f"Row(s) deleted from table *{table_name}*")
                self.examine_table(table_name)
            else:
                print("Operation canceled.")
        except Exception as e:
            raise Exception(f"Error while deleting row(s): {str(e)}")

    def close_conn(self, verbose=True):
        '''Closes the database connection when done'''
        try:
            self.conn.close()
            print(f"Closed connection to: {self.db_path}") if verbose else None
        except Exception as e:
            print(f"Error clearing the database: {str(e)}")

    def reconnect(self, database=None, rel_path=None, verbose=True):
        '''Reconnects to the current or a new database.'''
        old_db_path = self.db_path

        if database is not None:
            # Update db_path only if a new database name is provided
            if rel_path is not None:
                self.db_path = os.path.join(os.path.abspath(rel_path), database)
            else:
                self.db_path = os.path.join(os.path.abspath("../database/"), database)

        try:
            self.conn.close()  # Ensure the previous connection is closed
        except Exception:
            pass

        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            print(f"Connected to {self.db_path}") if verbose else None
        except Exception as e:
            print(f"Error trying to connect: {e}")
            self.db_path = old_db_path  # Restore the last valid path

    def clear_database(self, override=False):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")  # Get a list of all tables in the database
            tables = cursor.fetchall()
            _, file = os.path.split(self.db_path)
            if not override:  # Override confirmation to dispatch multiple databases (WARNING, abstract a confirmation check to a superior level)
                confirmation = input(f"Warning: This action will clear all data from the database {file}.\nDo you want to continue? (y/n): ").strip().lower()
            else:
                confirmation = "y"
            if confirmation == "y":
                for table in tables:  # Loop through the tables and delete them
                    table_name = table[0]
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                self.conn.commit()
                print(f"Database *{file}* cleared successfully.")
            else:
                print("Operation canceled.")
        except Exception as e:
            print(f"Error clearing the database: {str(e)}")

    """Internal methods"""
    def _input_handler(self, input):
        '''Modifies the input parameter to handle several types and always return an iterable'''
        if isinstance(input, str):
            input = [input]
            return input
        elif isinstance(input, (list, tuple, set)):
            return input
        else:
            raise Exception(f"Unsupported input format: Try str, list, tuple, set.")
################################################################################
################################################################################
class SQLite_Backup(SQLite_Handler):
    '''Automatic backup generator. Every time it runs it checks for an absolute 
    time condition comparing a .json file data with the specified backup time.'''
    def __init__(self, db_name, backup_folder=None, backup_time=None, rel_path=None):
        super().__init__(db_name, rel_path)  #Calls the parent class constructor
        if backup_folder is None: #Predefined path creation
            db_folder = os.path.abspath("../database/")
            self.backup_folder = os.path.join(db_folder, "backup")
        else: #Custom path creation
            self.backup_folder = os.path.realpath(backup_folder)
        print(f"Backup path: {self.backup_folder}")
        self.date, self.date_format = self._get_date(time.localtime())
        print(f"    Current time: {self.date_format}")
        name_without_extension, _ = os.path.splitext(db_name)
        name = name_without_extension + ".json"
        self.json_path = os.path.join(self.backup_folder, name) #Default json name
        if backup_time is None: 
            self.backup_time = 10800 #Default backup time
        else:
            self.backup_time = backup_time
        self.check_backup(db_name)

    def create_checkpoint(self, db_name=None):
        '''Creates a first backup and a .json file to store the backup info.
        A specific db can be set for executing the method'''
        _, db_path = self._build_paths(db_name)
        _, name = os.path.split(db_path)
        name_without_extension, _ = os.path.splitext(name)
        database = name
        filename = name_without_extension + ".json"
        date = self.date
        data = { #json data creation
            "database": database,
            "filename": filename,
            "date": date,
            "date_format": self.date_format
            }
        self.json_path = os.path.join(self.backup_folder, filename)
        if os.path.exists(self.json_path): #Ensures no accidental overwritting
            confirmation = input(f"Warning: There is a checkpoint for that database\nDo you want to overwrite it? (y/n): ").strip().lower()
            if confirmation == 'y':
                with open(self.json_path, "w") as json_file:
                    json.dump(data, json_file) #Write the data to the JSON file
                print(f"Checkpoint *{filename}* created for *{database}* at *{self.date_format}*")
                self._backup(db_name=name)
        else:
            with open(self.json_path, "w") as json_file:
                json.dump(data, json_file) #Write the data to the JSON file
            print(f"Checkpoint *{filename}* created for *{database}* at *{self.date_format}*")

    def manual_backup(self, db_name=None):
        '''Creates a manual backup by overwritting the json'''
        _, db_path = self._build_paths(db_name)
        _, name = os.path.split(db_path)
        try:
            with open(self.json_path, "r") as json_file:
                data = json.load(json_file)
                database = data["database"]
                filename = data["filename"]
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
        self.date, self.date_format = self._get_date(time.localtime())
        data = { #json data creation
            "database": database,
            "filename": filename,
            "date": self.date,
            "date_format": self.date_format
            }
        with open(self.json_path, "w") as json_file:
            json.dump(data, json_file) #Write the data to the JSON file
        print(f"Checkpoint *{filename}* created for *{database}* at *{self.date_format}*")
        self._backup(db_name=name)
    
    def check_backup(self, db_name):
        '''Quick auto-backup check'''
        _, db_path = self._build_paths(db_name) 
        folder_path, name = os.path.split(db_path)
        name_without_extension, _ = os.path.splitext(name)
        filename = name_without_extension + ".json"
        json_folder = os.path.join(folder_path, "backup")
        json_path = os.path.join(json_folder, filename)
        if not os.path.exists(json_path): #Creates a checkpoint if it doesn't exist yet.
            print(f"No checkpoint found: Creating *{filename}*")
            self.create_checkpoint(db_name)
        if self.backup_time == -1:
            print("Backup disabled. Add a valid time amount to start it.")
            return
        print(f"Backup time period: {self._format_time(self.backup_time)} HH:MM:SS")
        self._auto_backup(db_name)

    def promote(self, db_name=None, backup_name=None): #####Bugged
        '''Restores the desired backup. Will destroy the specified database to replace.'''
        if db_name is None:
            raise ValueError("No main db defined")
        if backup_name is None:
            raise ValueError("No backup db defined")
        db_folder, db_path = self._build_paths(db_name=db_name)
        backup_path = os.path.join(self.backup_folder, backup_name)
        name = os.path.splitext(db_name)[0]
        backup = os.path.splitext(backup_name)[0]
        confirmation = input(f"Warning: This action will replace {name} for {backup}.\nDo you want to continue? (y/n): ").strip().lower()
        if confirmation == 'y':
            self.close_conn()
            shutil.copy(backup_path, db_path)
            print(f"Backup {backup} restored")
            self.reconnect()

    '''Internal methods'''
    def _auto_backup(self, db_name):
        '''Checks if the backup condition is met and returns related information'''
        current_time, current_date_format = self._get_date(time.localtime()) #Calculates the current time
        try:
            with open(self.json_path, "r") as json_file:
                data = json.load(json_file)
                json_date = data["date"] #Gets the checkpoit date
            left_to_backup = self.backup_time - (current_time - json_date) #Calculates the time left
            if left_to_backup >= self.backup_time:
                self._backup(db_name=db_name)
                print(f"Auto-Backup created at {current_date_format}")
            else:
                print(f"Time to next backup: {self._format_time(left_to_backup)} HH:MM:SS")
        except:
            print("Auto-backup failed. Check if a ckeckpoint for the db is created.")

    def _backup(self, db_name=None):
        '''Creates the backup'''
        _, current_date_format = self._get_date(time.localtime())
        db_name, _ = os.path.splitext(db_name)
        backup_name = f"{db_name}_backup_{current_date_format}.db"
        backup_path = os.path.join(self.backup_folder, backup_name)
        backup_db = sqlite3.connect(backup_path) #Creates the backup db
        self.conn.backup(backup_db)
        backup_db.close()
        print(f"*{backup_name}* has been created.")

    def _get_date(self, time_struct):
        '''Gets the current date in both numeric and readable time'''
        min = time_struct.tm_min; sec = time_struct.tm_sec
        day = time_struct.tm_mday; hour = time_struct.tm_hour
        year = time_struct.tm_year; month = time_struct.tm_mon
        current_date = sec + min*60 + hour*3600 + day*86400 + month*2592000 + year*946080000
        current_date_format = f"{year}y-{month:02d}m-{day:02d}d_{hour}h-{min:02d}m-{sec:02d}s"
        return current_date, current_date_format

    def _build_paths(self, db_name=None):
        '''Builds predefined or specific paths for the database'''
        if db_name is None: #Gets the predefined database
            db_folder, db_name = os.path.split(self.db_path)
            db_path = self.db_path
        else: #Gets specified database
            db_folder = os.path.dirname(self.db_path)
            db_path = os.path.join(db_folder, db_name)
        return db_folder, db_path

    def _format_time(self, seconds):
        '''Formats time in a HH:MM:SS fashion or converts "HH:MM:SS" string to seconds.'''
        if isinstance(seconds, (int, float)):
            hours, remainder = divmod(seconds, 3600)  # 3600 seconds in an hour
            minutes, seconds = divmod(remainder, 60)  # 60 seconds in a minute
            formatted_time = f'{hours:02d}:{minutes:02d}:{seconds:02d}'
            return formatted_time
        elif isinstance(seconds, str):
            try:
                parts = seconds.split(':')
                hours, minutes, seconds = map(int, parts)
                total_seconds = hours * 3600 + minutes * 60 + seconds
                return total_seconds
            except Exception as e:
                raise Exception(f"Error while formatting the string: {e}")
        else:
            raise ValueError("Invalid input type. Use either int or 'HH:MM:SS' string for time input.")

###File Structure
#-project_root
#---data (raw data)
#
#---database (database location)
#------database.db
#------backup
#---------checkpoint.json
#---------database_backup.db
#
#---src (actual project)
#------main.py

###Test script
if __name__ == '__main__':
    ...