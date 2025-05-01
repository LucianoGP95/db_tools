import os, sys, re
import pandas as pd
from urllib.parse import urlparse
from sqlite_handler import SQLite_Handler

class SQLite_Data_Extractor(SQLite_Handler):
    '''Extracts structured data from different sources and turns it into a table in a database
    for quick deployment. Creates a db from raw data or adds tables to it from raw data.'''
    def __init__(self, db_name, db_folder_path=None, source_folder_path=None, rel_path=False):
        super().__init__(db_name, db_folder_path, rel_path)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        # Handle source folder path logic
        if source_folder_path is None:
            # Default to ../data/
            self.source_folderpath = os.path.abspath(os.path.join(base_dir, "../data/"))
        elif isinstance(source_folder_path, str) and (os.path.isabs(source_folder_path) or os.path.sep in source_folder_path):
            # Treat as full or relative path
            self.source_folderpath = os.path.abspath(source_folder_path)
        elif rel_path and source_folder_path:
            # Relative path from base_dir
            self.source_folderpath = os.path.abspath(os.path.join(base_dir, source_folder_path))
        else:
            raise ValueError("Invalid combination of source_folder_path and rel_path")
        os.makedirs(self.source_folderpath, exist_ok=True)
        print(f"✅ Data source folder: {self.source_folderpath}")
        # Optional config
        self.source_name = None
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