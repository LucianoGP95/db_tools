#V22.0 17/04/2025
import os, json, time, re, sys, shutil, sqlite3
from urllib.parse import urlparse
import pandas as pd
from query_builder import QueryBuilder
#Secondary requirements: pip install openpyxl
################################################################################
class SQLite_Handler:
    '''SQLite custom handler'''
    def __init__(self, db_name: str, rel_path=None):
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # Memory database shortcut
        if db_name == ":memory:":
            self.db_path = ":memory:"
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            print("Test database created in RAM")
            return

        # Set the path
        if rel_path is None:
            db_dir = os.path.abspath(os.path.join(base_dir, "../database/"))
        else:
            try:
                db_dir = os.path.abspath(os.path.join(base_dir, rel_path))
            except Exception as e:
                raise OSError(f"Error resolving path: {e}")

        os.makedirs(db_dir, exist_ok=True)  # Ensure the directory exists
        self.db_path = os.path.join(db_dir, db_name)

        # Connect and log
        if not os.path.exists(self.db_path):
            print(f"Database *{db_name}* created in: {self.db_path}")
        else:
            print(f"Database *{db_name}* found in: {self.db_path}")

        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        print(f"Database *{db_name}* connected.")

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

    def migrate_table(self, table_name: str, verbose: bool=True, foreign_key: str=None):
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

    def delete_row(self, row_name: str, table_name: str):
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

    def pragma_conf(self, foreign_keys: bool=False):
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
class SQLite_Data_Extractor(SQLite_Handler):
    '''Extracts structured data from different sources and turns it into a table in a database for quick deployment. Creates a db 
    from raw data or adds tables to it from raw data'''
    def __init__(self, db_name, db_rel_filepath=None, source_rel_folderpath=None):
        super().__init__(db_name, db_rel_filepath)  #Calls the parent class constructor
        self.source_name = None
        if source_rel_folderpath is None:
            self.source_folderpath: str = os.path.abspath("../data/")
        else:  # Optional relative path definition
            try:
                self.source_folderpath: str = os.path.abspath(source_rel_folderpath)
            except OSError as e:
                print(f"Error with custom path creation: {e}")
        print(f"Data source folder: {os.path.abspath(source_rel_folderpath)}")
        self.add_index = False
        self.sep = ","

    def store(self, source):
        '''Generates table(s) of the given name using data from different sources'''
        self.source_name = source
        self._inputhandler() # Handles the source input format
        # Proccess data based of extension:
        self._input_type_workflow()
        try: # Incase there is a problem with the parent method
            self.consult_tables()
        except Exception as e:
            pass

    def store_directory(self, input_rel_path=None):
        '''Generates table(s) for all the compatible files inside the custom directory. If the directory isn't given, it uses 
        ../data/'''
        if input_rel_path:
            try: #Check if the directory exists, and create it if it doesn't
                directory_path = os.path.abspath(input_rel_path)
                if not os.path.exists(directory_path):
                    os.makedirs(directory_path)
                self.source_path = [os.path.join(directory_path, name) for name in os.listdir(directory_path)]
            except Exception as e:
                print(f"Error creating or accessing custom directory '{directory_path}': {e}")
                print("    The operation has been canceled.")
                sys.exit(1)
        else:
            default_directory = os.path.abspath("../data/")
            try: #Check if the default directory exists, and create it if it doesn't
                if not os.path.exists(default_directory):
                    os.makedirs(default_directory)
                self.source_path = [os.path.join(default_directory, name) for name in os.listdir(default_directory)]
            except Exception as e:
                print(f"Error creating or accessing default directory '{default_directory}': {e}")
                print("    The operation has been canceled.")
                sys.exit(1)
        # Proccess data based of extension:
        self._input_type_workflow()
        try:  
            self.consult_tables()
        except Exception as e: #In case there is a problem with the parent method
            pass

    def store_df(self, df, table_name=None):
        '''Stores the desired dataframe as a table in the connected database.'''
        if table_name is not None:
            try:
                table_name = re.sub(r'\W', '_', table_name) #Replace non-alphanumeric characters with underscores in table_name
                self.df = df
                self.df.to_sql(table_name, self.conn, if_exists='replace', index=self.add_index)
                self.conn.commit()
                print(f"Dataframe stored as *{table_name}*")
            except Exception as e:
                print(f"Error storing the dataframe: {str(e)}")
        else: 
            try:
                table_name = f"Exported_df"
                self.df = df
                self.df.to_sql(table_name, self.conn, if_exists='fail', index=self.add_index)
                self.conn.commit()
                print(f"Dataframe stored as *{table_name}*")
            except Exception as e:
                print(f"Error storing the dataframe: {str(e)}\n Try adding the parameter table_name.")

    def retrieve(self, table_name):
        '''Retrieves a table from the database as a dataframe object. If the arg. is a list or tuple it will try to concatenate
        all the tables'''
        self.index_col = None if not hasattr(self, 'index_col') else self.index_col
        if isinstance(table_name, str):
            try:
                self.cursor = self.conn.cursor()
                query = f"SELECT * FROM {table_name}"
                self.df = pd.read_sql(query, self.conn, index_col=self.index_col)
                print(f"Table *{table_name}* retrieved succesfully.")
                return self.df
            except Exception as e:
                print(f"Error retrieving table as dataframe: {str(e)}")
                return None
        if isinstance(table_name, (list, tuple)):
            dataframes = []
            for table in table_name:
                try:
                    self.cursor = self.conn.cursor()
                    query = f"SELECT * FROM {table}"
                    df = pd.read_sql(query, self.conn, index_col=self.index_col)
                    dataframes.append(df)
                    print(f"Table {table} retrieved succesfully.")
                except Exception as e:
                    print(f"Error retrieving table as dataframe: {str(e)}")
                    return None
            try:
                self.df = pd.concat(dataframes, ignore_index=True)
            except Exception as e:
                print(f"Error concatenating dataframes: {str(e)}")
        return self.df

    def set_rules(self, sep=None, add_index=False, index_col=None, verbose=False):
        '''Used to modify the rules that pandas uses to parse files.'''
        self.index_col = index_col
        self.add_index = add_index
        self.sep = "," if sep is None else sep
        if isinstance(self.sep, (str,)) and self.sep in (",", ".", " "):
            print(f"Updated rules:\nSeparator set to:{self.sep}") if verbose == True else None
        else:
            self.sep = ","
            print(f"Error changing the rules: Unsupported separator.\nSeparator set to:{self.sep}")

    def set_default_rules(self, verbose=False):
        '''Sets or resets all rules to default.'''
        self.index_col = None
        self.add_index = False
        self.sep = ","
        if verbose == True:
            print(f"Object rules set to default:\nindex_col={self.index_col}\nadd_index={self.add_index}\nsep={self.sep }")

    def delete_table(self, table_name):
        super().delete_table(table_name) 

    def examine_table(self, table_name):
        super().examine_table(table_name) 

    '''Internal methods'''
    def _inputhandler(self):
        '''Handles variable quantity of elements. It accepts: 
            - A list or tuple indicating the desired files in ../data/
            - A string indicating a single file in ../data/
            - An url with a supported filetype'''
        self.flag = self._is_url(self.source_name) #Determines if the given source is an url
        if self.flag: 
            self.source_path = [self.source_name] #Converts the string to list to allow iteration with 1 element.
            print("url detected")
        else:
            if isinstance(self.source_name, str): 
                self.source_path = os.path.join(self.source_folderpath, self.source_name)
                self.source_path = [self.source_path] #Converts the string to list to allow iteration with 1 element.
            elif isinstance(self.source_name, (list, tuple)):
                self.source_path = [self.source_rel_path + name for name in self.source_name]
            else:
                raise Exception(f"Error importing data: Data mas be specified in str, list or tuple format") 

    def _input_type_workflow(self):        
        for index, source_path in enumerate(self.source_path):
            source_path = os.path.abspath(source_path)
            self._filetypehandler(source_path)  #Handles the filetype
            if self.extension == "xlsx":
                self._datasheet_excel(index)
            if self.extension == "csv":
                self._datasheet_csv(index)
            if self.extension == "json":
                self._datasheet_json(index)

    def _filetypehandler(self, source_path):
        '''Handles all the supported filetypes. Currently supported:
        - .csv
        - .xlsx (Excel)
        - .json
        - An URL pointing to a file of the above'''
        
        self.extension = source_path.split(".")[-1].lower()  # Get file extension, case-insensitive

        match self.extension:
            case "xlsx":
                try:
                    self.df = pd.read_excel(source_path, sheet_name=None)  # Dictionary of DataFrames
                except Exception as e:
                    raise Exception(f"Error importing Excel file into pandas: {str(e)}")

            case "csv":
                try:
                    self.df = pd.read_csv(source_path, header=0, sep=self.sep)
                except Exception as e:
                    raise Exception(f"Error importing CSV into pandas: {str(e)}")

            case "json":
                try:
                    import json
                    with open(source_path, "r", encoding="utf-8") as f:
                        raw = json.load(f)

                    df = pd.json_normalize(raw)

                    def clean_value(val):
                        if isinstance(val, list):
                            return ", ".join(str(v) for v in val)
                        elif isinstance(val, dict):
                            return json.dumps(val, ensure_ascii=False)
                        else:
                            return val

                    df = df.applymap(clean_value)

                    if df.empty or len(df.columns) == 0:
                        raise Exception("El DataFrame resultante está vacío o no tiene columnas.")

                    self.df = df

                except Exception as e:
                    raise Exception(f"Error importando JSON como DataFrame: {str(e)}")

    def _datasheet_excel(self, i):
        '''Specific method for sending .xlsx files with all their sheets as tables in the db'''
        try:
            _, source_name = os.path.split(self.source_name)
            source_name, _ = os.path.splitext(source_name)
            print(f'Data from *{source_name}* has been imported to {self.db_path}')
            print(f"Sheet(s) imported to db as table(s) with name(s):")
            j = 0
            for sheet_name, sheet in self.df.items():
                j += 1
                if len(self.df.items()) == 1: #Name for single sheet excels
                    table_name = self._sanitize_name(source_name, i)
                else:
                    table_name = self._sanitize_name(sheet_name, i)
                if not table_name[0].isalpha() and table_name[0] != '_': #Ensure the table_name starts with a letter or underscore
                    table_name = f"xlsx_table{j}"
                    print(f"Invalid table name for sheet: *{sheet_name}*. Adding it as *{table_name}*")
                print(f"    {table_name}")
                sheet.to_sql(table_name, self.conn, if_exists='replace', index=self.add_index)
        except Exception as e:
            raise Exception(f"Error connecting to database: {str(e)}")

    def _datasheet_csv(self, i):
        '''Specific method for sending .csv files as tables in the db'''
        try:
            _, source_name = os.path.split(self.source_name)
            source_name, _ = os.path.splitext(source_name)
            # Clean up table name
            table_name = self._sanitize_name(source_name, i)
            print(f'Data from *{source_name}* has been imported to {self.db_path}.')
            print(f"    {table_name}")
            self.df.to_sql(table_name, self.conn, if_exists='replace', index=False)
        except Exception as e:
            raise Exception(f"Error connecting to database: {str(e)}")

    def _datasheet_json(self, i):
        '''Specific method for sending .json files as tables in the db'''
        try:
            _, source_name = os.path.split(self.source_name)
            source_name, _ = os.path.splitext(source_name)
            
            # Clean up table name
            table_name = self._sanitize_name(source_name, i)

            # Show feedback
            print(f'Data from *{source_name}* has been imported to {self.db_path}.')
            print(f"    {table_name}")
            
            # Insert into DB
            self.df.to_sql(table_name, self.conn, if_exists='replace', index=False)
        except Exception as e:
            raise Exception(f"Error connecting to database: {str(e)}")

    def _sanitize_name(self, source_name: str, i) -> str:
        table_name = re.sub(r'\W', '_', source_name)
        if not table_name[0].isalpha() and table_name[0] != '_':
            table_name = f"json_table{i+1}"
            print(f"Invalid table name: *{table_name}*. Adding it as *table{i+1}*")
        return table_name

    def _is_url(self, string):
        '''Determines whether the given argument is an url or not'''
        try:
            result = urlparse(string)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False
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
    #Creates or connects to a db in ../database/
    dbh = SQLite_Data_Extractor("sigma_values.db", rel_path=None)
    dbh.set_default_rules(verbose=True)
    #Save a specific file inside ../data/
    dbh.store("sigma.csv")
    #Info of all tables
    dbh.consult_tables()
    #Show info and the contents of specific tables
    dbh.examine_table(["sigma"])
    #Rename a table
    dbh.rename_table("test1", "new_test")
    #Get a table into a dataframe
    df = dbh.retrieve("new_test")
    #Close the connection when done
    dbh.close_conn()
    #Reconnect to the actual db or a new one
    dbh.reconnect()
    #Store the whole ../data/ directory or a custom one
    dbh.store_directory()
    
    #Create the backup manager
    bc = SQLite_Backup("database.db", backup_time=10800)
    #Create a checkpoint for the database to measure time since the last backup
    bc.create_checkpoint("database.db")
    #Generate a manual backup
    bc.manual_backup("database.db")
    #Check for the auto-backup
    bc.check_backup("database.db")
    #Restore a specific database backup. Requires both names input for safety.
    bc.promote("database.db", "database_backup_2023y-10m-20d_11h-58m-01s.db")
    
    ###WARNING zone###
    #Delete row(s)
    dbh.delete_row('a', "new_test")
    #Delete a single table
    dbh.delete_table("new_test")
    #Clear the database
    dbh.clear_database()