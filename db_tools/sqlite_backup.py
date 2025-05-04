import os, json, time, shutil, sqlite3
from sqlite_handler import SQLite_Handler

class SQLite_Backup(SQLite_Handler):
    '''Automatic backup generator. Every time it runs it checks for an absolute 
    time condition comparing a .json file data with the specified backup time.'''
    
    def __init__(self, db_name: str, backup_folder=None, backup_time=None, db_folder_path: str = None, rel_path: bool = False):
        # Call the parent class constructor to use its path logic
        super().__init__(db_name, db_folder_path, rel_path)
        
        # Setup backup folder
        if backup_folder is None:
            if self.db_path == ":memory:":
                # For memory databases, use the default database directory from SQLite_Handler
                base_dir = os.path.dirname(os.path.abspath(__file__))
                db_dir = os.path.abspath(os.path.join(base_dir, "../database/"))
            else:
                # For file databases, use the same directory as the database
                db_dir = os.path.dirname(self.db_path)
            self.backup_folder = os.path.join(db_dir, "backup")
        else:
            self.backup_folder = os.path.abspath(backup_folder)
        
        # Create backup folder if it doesn't exist
        os.makedirs(self.backup_folder, exist_ok=True)
        
        print(f"Backup path: {self.backup_folder}")
        self.date, self.date_format = self._get_date(time.localtime())
        print(f"    Current time: {self.date_format}")
        
        # Set up json path for checkpoint
        if self.db_path == ":memory:":
            name_without_extension = "memory_db"
        else:
            db_basename = os.path.basename(self.db_path)
            name_without_extension, _ = os.path.splitext(db_basename)
        
        json_filename = name_without_extension + ".json"
        self.json_path = os.path.join(self.backup_folder, json_filename)
        
        # Set backup time
        if backup_time is None: 
            self.backup_time = 10800  # Default backup time (3 hours)
        else:
            self.backup_time = backup_time
        
        self.check_backup(db_name)

    def create_checkpoint(self, db_name=None):
        '''Creates a first backup and a .json file to store the backup info.
        A specific db can be set for executing the method'''
        # Use the original db if none specified
        if db_name is None:
            db_path = self.db_path
            db_name = os.path.basename(self.db_path) if self.db_path != ":memory:" else "memory_db"
        else:
            # If a different db is specified, leverage parent class path logic
            temp_handler = SQLite_Handler(db_name)
            db_path = temp_handler.db_path
            temp_handler.close_conn(verbose=False)
            db_name = os.path.basename(db_path) if db_path != ":memory:" else "memory_db"
        
        # Extract names for JSON
        if db_path == ":memory:":
            name_without_extension = "memory_db"
        else:
            name_without_extension, _ = os.path.splitext(db_name)
        
        filename = name_without_extension + ".json"
        date = self.date
        
        data = {
            "database": db_name,
            "filename": filename,
            "date": date,
            "date_format": self.date_format
        }
        
        self.json_path = os.path.join(self.backup_folder, filename)
        
        if os.path.exists(self.json_path):
            confirmation = input(f"Warning: There is a checkpoint for that database\nDo you want to overwrite it? (y/n): ").strip().lower()
            if confirmation == 'y':
                with open(self.json_path, "w") as json_file:
                    json.dump(data, json_file)
                print(f"Checkpoint *{filename}* created for *{db_name}* at *{self.date_format}*")
                self._backup(db_path)
            else:
                print("Operation canceled.")
        else:
            with open(self.json_path, "w") as json_file:
                json.dump(data, json_file)
            print(f"Checkpoint *{filename}* created for *{db_name}* at *{self.date_format}*")
            self._backup(db_path)

    def manual_backup(self, db_name=None):
        '''Creates a manual backup by overwriting the json'''
        # Use the original db if none specified
        if db_name is None:
            db_path = self.db_path
        else:
            # If a different db is specified, leverage parent class path logic
            temp_handler = SQLite_Handler(db_name)
            db_path = temp_handler.db_path
            temp_handler.close_conn(verbose=False)
        
        try:
            with open(self.json_path, "r") as json_file:
                data = json.load(json_file)
                database = data["database"]
                filename = data["filename"]
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
            return
            
        self.date, self.date_format = self._get_date(time.localtime())
        data = {
            "database": database,
            "filename": filename,
            "date": self.date,
            "date_format": self.date_format
        }
        
        with open(self.json_path, "w") as json_file:
            json.dump(data, json_file)
            
        print(f"Checkpoint *{filename}* created for *{database}* at *{self.date_format}*")
        self._backup(db_path)
    
    def check_backup(self, db_name):
        '''Quick auto-backup check'''
        # Use current connection if no db_name specified
        if db_name is None or (isinstance(db_name, str) and db_name == os.path.basename(self.db_path)):
            db_path = self.db_path
        else:
            # If a different db is specified, leverage parent class path logic
            temp_handler = SQLite_Handler(db_name)
            db_path = temp_handler.db_path
            temp_handler.close_conn(verbose=False)
        
        if db_path == ":memory:":
            name_without_extension = "memory_db"
        else:
            db_basename = os.path.basename(db_path)
            name_without_extension, _ = os.path.splitext(db_basename)
        
        filename = name_without_extension + ".json"
        self.json_path = os.path.join(self.backup_folder, filename)
        
        if not os.path.exists(self.json_path):
            print(f"No checkpoint found: Creating *{filename}*")
            self.create_checkpoint(db_name)
            return
            
        if self.backup_time == -1:
            print("Backup disabled. Add a valid time amount to start it.")
            return
            
        print(f"Backup time period: {self._format_time(self.backup_time)} HH:MM:SS")
        self._auto_backup(db_path)

    def promote(self, db_name=None, backup_name=None):
        '''Restores the desired backup. Will destroy the specified database to replace.'''
        if db_name is None:
            db_path = self.db_path
        else:
            # Create temporary handler to get path using parent class logic
            temp_handler = SQLite_Handler(db_name)
            db_path = temp_handler.db_path
            temp_handler.close_conn(verbose=False)
            
        if backup_name is None:
            raise ValueError("No backup db filename defined")
            
        # Make sure backup_name has .db extension
        if not backup_name.lower().endswith('.db'):
            backup_name += '.db'
            
        backup_path = os.path.join(self.backup_folder, backup_name)
        
        # Check if backup exists
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"Backup file {backup_name} not found in {self.backup_folder}")
        
        # Get names for confirmation
        if db_path == ":memory:":
            name = "memory_db"
        else:
            name = os.path.splitext(os.path.basename(db_path))[0]
            
        backup = os.path.splitext(backup_name)[0]
        
        confirmation = input(f"Warning: This action will replace {name} for {backup}.\nDo you want to continue? (y/n): ").strip().lower()
        if confirmation == 'y':
            # If we're restoring to the current database
            if db_path == self.db_path:
                self.close_conn()
                if db_path != ":memory:":  # Can't copy to memory
                    shutil.copy(backup_path, db_path)
                    print(f"Backup {backup} restored to {db_path}")
                else:
                    print("Cannot restore a file backup to an in-memory database")
                    print("Will connect to the backup instead")
                    self.db_path = backup_path
                self.reconnect()
            else:
                # If restoring to a different database than current connection
                if db_path != ":memory:":  # Can't copy to memory
                    shutil.copy(backup_path, db_path)
                    print(f"Backup {backup} restored to {db_path}")
                else:
                    print("Cannot restore a file backup to an in-memory database")
        else:
            print("Operation canceled.")

    '''Internal methods'''
    def _auto_backup(self, db_path):
        '''Checks if the backup condition is met and creates a backup if needed'''
        current_time, current_date_format = self._get_date(time.localtime())
        
        try:
            with open(self.json_path, "r") as json_file:
                data = json.load(json_file)
                json_date = data["date"]
                
            # Calculate time elapsed since last backup
            time_elapsed = current_time - json_date
            
            # Check if it's time for a backup
            if time_elapsed >= self.backup_time:
                self._backup(db_path)
                
                # Update JSON with new time
                data["date"] = current_time
                data["date_format"] = current_date_format
                
                with open(self.json_path, "w") as json_file:
                    json.dump(data, json_file)
                    
                print(f"Auto-Backup created at {current_date_format}")
            else:
                time_remaining = self.backup_time - time_elapsed
                print(f"Time to next backup: {self._format_time(time_remaining)} HH:MM:SS")
        except Exception as e:
            print(f"Auto-backup failed: {e}. Check if a checkpoint for the db is created.")

    def _backup(self, db_path):
        '''Creates the backup'''
        _, current_date_format = self._get_date(time.localtime())
        
        if db_path == ":memory:":
            db_name = "memory_db"
        else:
            db_basename = os.path.basename(db_path)
            db_name, _ = os.path.splitext(db_basename)
            
        backup_name = f"{db_name}_backup_{current_date_format}.db"
        backup_path = os.path.join(self.backup_folder, backup_name)
        
        # Handle memory database case
        if db_path == ":memory:":
            backup_db = sqlite3.connect(backup_path)
            self.conn.backup(backup_db)
            backup_db.close()
            print(f"*{backup_name}* has been created from memory database.")
        else:
            # Handle file database case
            is_current_db = (db_path == self.db_path)
            
            try:
                # Close connection if it's our current database
                if is_current_db:
                    self.conn.commit()
                    self.conn.close()
                
                # Create a copy of the database file
                shutil.copy2(db_path, backup_path)
                
                # Reconnect if we closed our connection
                if is_current_db:
                    self.reconnect(verbose=False)
                    
                print(f"*{backup_name}* has been created.")
            except Exception as e:
                print(f"Error creating backup: {e}")
                # Ensure connection is restored
                if is_current_db:
                    self.reconnect(verbose=False)

    def _get_date(self, time_struct):
        '''Gets the current date in both numeric and readable time'''
        min = time_struct.tm_min; sec = time_struct.tm_sec
        day = time_struct.tm_mday; hour = time_struct.tm_hour
        year = time_struct.tm_year; month = time_struct.tm_mon
        
        # Calculate seconds since epoch for easy comparison
        current_date = sec + min*60 + hour*3600 + day*86400 + month*2592000 + year*946080000
        
        # Format date as string
        current_date_format = f"{year}y-{month:02d}m-{day:02d}d_{hour}h-{min:02d}m-{sec:02d}s"
        
        return current_date, current_date_format

    def _format_time(self, seconds):
        '''Formats time in a HH:MM:SS fashion or converts "HH:MM:SS" string to seconds.'''
        if isinstance(seconds, (int, float)):
            # Ensure seconds is non-negative
            seconds = max(0, int(seconds))
            
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