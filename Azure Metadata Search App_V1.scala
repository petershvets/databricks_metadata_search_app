// Databricks notebook source
// DBTITLE 1,1. set up user input
// MAGIC %python
// MAGIC dbutils.widgets.text("v_dbname", "", label = "Database Name")
// MAGIC dbutils.widgets.text("v_tblname", "", label = "Table Name")
// MAGIC dbutils.widgets.text("v_colname", "", label = "Column Name")

// COMMAND ----------

// MAGIC %python
// MAGIC dbnamepattern = dbutils.widgets.get("v_dbname").upper()
// MAGIC tblnamepattern = dbutils.widgets.get("v_tblname").upper()
// MAGIC colnamepattern = dbutils.widgets.get("v_colname").upper()
// MAGIC
// MAGIC if dbnamepattern: print("Lookup Database name: "+dbnamepattern)
// MAGIC if tblnamepattern: print("Lookup Table name: "+tblnamepattern)
// MAGIC if colnamepattern: print("Lookup Column name: "+colnamepattern)

// COMMAND ----------

// DBTITLE 1,Data Dictionary Search Application
// MAGIC %python
// MAGIC # Define service functions
// MAGIC # List all databases available in the cluster
// MAGIC def get_all_db():
// MAGIC   databases_df = spark.sql("show databases")
// MAGIC   dbList = [x["databaseName"] for x in databases_df.rdd.collect()]
// MAGIC   # check db list is not empty and handle return correctly
// MAGIC   if dbList:
// MAGIC     return dbList
// MAGIC   else:
// MAGIC     return False
// MAGIC
// MAGIC def get_all_tables():
// MAGIC   # get all available databases in the cluster
// MAGIC   dblist = get_all_db()
// MAGIC   all_tbllist = list()
// MAGIC   tablesdb = list()
// MAGIC   # Iterate throug all databases and get all tables for every database in the cluster
// MAGIC   if dblist: # make sure the list is not empty
// MAGIC     for db in dblist:
// MAGIC       print("DEBUG: Processing database: "+ db)
// MAGIC       # get tables for current database
// MAGIC       tablesdb = get_table(db)
// MAGIC       for dbtbl in tablesdb:
// MAGIC         print("    DEBUG: Appending tables: "+dbtbl)
// MAGIC         all_tbllist.append(dbtbl)
// MAGIC       # clear tables list for current iteration
// MAGIC       tablesdb.clear()
// MAGIC     return all_tbllist
// MAGIC   else:
// MAGIC     return False
// MAGIC   
// MAGIC def get_table(dbname):
// MAGIC   tables_df = sqlContext.sql("show tables in "+dbname)
// MAGIC  # tableList = [x["tableName"] for x in tables_df.rdd.collect()]
// MAGIC   tableList = [x["database"]+"."+x["tableName"] for x in tables_df.rdd.collect()]
// MAGIC   return tableList
// MAGIC
// MAGIC def table_lookup(tbname):
// MAGIC   # get all available databases in the cluster
// MAGIC   dblist = get_db()
// MAGIC   # Initialize list of tables
// MAGIC   tbllist = list()
// MAGIC   # get all table names across all databases in the cluster
// MAGIC   for db in dblist:
// MAGIC     # Get list of tables for database
// MAGIC     table_list(db)
// MAGIC
// MAGIC def column_lookup():
// MAGIC   # get all available databases in the cluster
// MAGIC   dblist = get_db()
// MAGIC   
// MAGIC   
// MAGIC def main():
// MAGIC   # Process user input
// MAGIC   dbnamepattern = dbutils.widgets.get("v_dbname").upper()
// MAGIC   tblnamepattern = dbutils.widgets.get("v_tblname").upper()
// MAGIC   colnamepattern = dbutils.widgets.get("v_colname").upper()
// MAGIC
// MAGIC   if dbnamepattern: print("Lookup Database name: "+dbnamepattern)
// MAGIC   if tblnamepattern: print("Lookup Table name: "+tblnamepattern)
// MAGIC   if colnamepattern: print("Lookup Column name: "+colnamepattern)
// MAGIC   # TODO create RegEx expressions to match on db, tbl or col name patterns
// MAGIC
// MAGIC   # Branch execution based on user input
// MAGIC   # If user did not define any search parameters, show all databases and tables
// MAGIC   if (not dbnamepattern and not tblnamepattern and not colnamepattern):
// MAGIC     print("User provided not input. All databases and tables will be displayed")
// MAGIC #     dblist = get_all_db()
// MAGIC #     if dblist:
// MAGIC #       for db in dblist:
// MAGIC #         print("DB name: "+db)
// MAGIC     allbllist = get_all_tables()
// MAGIC     for tbl in allbllist:
// MAGIC       print(tbl)
// MAGIC   # If user defined onlty database name, show all tables in this database
// MAGIC   if dbnamepattern and (not tblnamepattern and not colnamepattern):
// MAGIC     dbtablelist = get_table(dbnamepattern)
// MAGIC     for tbl in dbtablelist:
// MAGIC       print("Found table: "+tbl)
// MAGIC           
// MAGIC     
// MAGIC # Main execution.
// MAGIC if __name__ == '__main__':   
// MAGIC     # Run main
// MAGIC     main()
// MAGIC

// COMMAND ----------

// DBTITLE 1,cleanup widgets
// MAGIC %python
// MAGIC dbutils.widgets.removeAll()
