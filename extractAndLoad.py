# Import Libraries
from pyspark.sql import SparkSession
import pyodbc


def extract_and_load_data(spark, tablename_list,database):
    """_summary_
        extract data from postgresql and load in SQL server database
    Args:
        spark (object): Spark connection 
        tablename_list (list): table list name 
        database (string): database we extract the data from
    """
    jdbcUrl = "jdbc:sqlserver://DESKTOP-L3KHVFJ\SQLEXPRESS:1433;database=businesslocation;trustServerCertificate=true;"

    reader = (
        spark.read.format('jdbc')
        .option("url", f"jdbc:postgresql://localhost:5432/{database}")
        .option('user', 'username')
        .option('password', 'password')
        .option("driver", "org.postgresql.Driver")
    )


    for tablename in tablename_list:
        # Read Data From PostgreSql
        table_reader = reader.option("dbtable", tablename).load()
        # write Data to Sql Server
        table_reader.write.format("jdbc")\
            .option("url", jdbcUrl)\
            .option("user", "username")\
            .option("password", "password")\
            .option("dbtable", tablename).save()

# Create SQLServer DataBaase
def create_sqlserver_database(database):
    """
    Create SQL Server database

    Args:
        database (string): Database Name
    """
    cnxn_str = ("Driver={SQL Server Native Client 11.0};"
                "Server=DESKTOP-L3KHVFJ\SQLEXPRESS;""Database=;""Trusted_Connection=yes;")
    conn = pyodbc.connect(cnxn_str, autocommit=True)
    cursor = conn.cursor()
    print(cursor.execute(f"CREATE DATABASE {database}"))
    conn.close()


def main():
    print("Create Spark Session.....")
    spark = SparkSession.builder.getOrCreate()  # Create Spark Session
    tablename_list = ['locationtime', 'businesstime',
                      'registeredbusiness', 'location']
    print("Create SQL Server Database........")                  
    create_sqlserver_database("businesslocation") 
    print("Extract and Load data from postgreSQL to SQlServer........")                  
    extract_and_load_data(spark,tablename_list,'businesslocation')  
    spark.stop()               
    print("Congratelaution finish Extract and Load........")                  

if __name__ == '__main__':
    main()
