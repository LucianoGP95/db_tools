from db_tools import SQLite_Data_Extractor, SQLite_Backup
import os

os.chdir(os.path.dirname(os.path.abspath(__file__))) # Sets the cwd


#Creates or connects to a db in ../database/
dbh = SQLite_Data_Extractor(":memory:")
#Save a specific file inside ../data/
dbh.store("../raw_data/data.csv")
#Info of all tables
dbh.consult_tables()
""" #Show info and the contents of specific tables
dbh.examine_table(["test1", "test2"])
#Rename a table
dbh.rename_table("test1", "renamed_table")
#Get a table into a dataframe
df = dbh.retrieve("renamed_table")
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
#Restore a specific database backup. Requires both names for safety.
bc.promote("database.db", "database_backup_2023y-10m-20d_11h-58m-01s.db")

###WARNING zone###
#Delete row(s)
dbh.delete_row('a', "new_test")
#Delete a single table
dbh.delete_table("new_test")
#Clear the database
dbh.clear_database() """