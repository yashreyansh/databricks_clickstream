from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import os

def file_processing_check(name):
    if spark.catalog.tableExists('workspace.bronze_click.file_load'):
        pass
    else:
        spark.sql('''
                  CREATE TABLE workspace.bronze_click.file_load
                  (
                  file_name string,
                  load_time timestamp,
                  status string
                  )
                  USING DELTA
                  ''')
    flag = spark.sql(f'''
              select count(*) as cnt from workspace.bronze_click.file_load
              where file_name = '{name}' and status = 'processed'
              ''').collect()[0]["cnt"]
    if flag==1:      
        return "processed"
    else:
        return "not processed"


def get_All_parquet(volume):
    parquet_files = []
    for root, dirs, files in os.walk(volume):
        for file in files:
            if file.lower().endswith(".parquet"):
                full_path = os.path.join(root, file)
                parquet_files.append(full_path)
    return parquet_files


def main():
    volume = '/Volumes/workspace/bronze_click/bronze_raw_data/'
    bronze_table = 'workspace.bronze_click.bronze_raw_data'
    file_load_table = 'workspace.bronze_click.file_load'
    parquet_files= get_All_parquet(volume)  # list

    data_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("UID", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("session_id", StringType(), True),
        StructField("page", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("latency_ms", IntegerType(), True),
        StructField("created_on", TimestampType(), True),
        StructField("is_loaded",  IntegerType(), True),
        StructField("file_name", StringType(), True),
        ])

    # creating empty temp df
    current_files_temp_df = spark.createDataFrame([], data_schema)
    if spark.catalog.tableExists(bronze_table):
        print("Bronze table already exists... moving to append the data.")
        pass
    else:
        current_files_temp_df.createOrReplaceTempView("bronze_df")
        spark.sql('''
                CREATE OR REPLACE TABLE IDENTIFIER(:target)
                    USING DELTA
                    AS
                    SELECT *
                    FROM bronze_df
                ''', 
                args={"target":bronze_table}
                 )
        
        print("Table didn't exists.... created now")


    files_to_load = []
    # loading each file record to temp df
    for file in parquet_files:
        temp = spark.read\
            .format("parquet")\
            .schema(data_schema)\
            .load(file)

        # checking if file is already laoded
        if file_processing_check(file)=="processed":
            print(f"Skipping file: {file} as its already processed")
            continue
        files_to_load.append(file)
        temp = temp.withColumn("is_loaded", lit(0))
        temp = temp.withColumn("created_on", current_timestamp())
        temp = temp.withColumn("file_name", lit(file.split('/')[-1]))
        temp = temp.withColumn("UID",md5(concat_ws("|", "user_id", "ts","device_type","file_name","latency")))
        current_files_temp_df = current_files_temp_df.unionByName(temp)
        print(f"Loading file: {file}") 

    # Write to table
    '''
    delta_tbl = DeltaTable.forName(spark, bronze_table)

    delta_tbl.alias('target').merge(
        current_files_temp_df.alias("temp"),
        "
        target.user_id=temp.user_id AND target.ts=temp.ts AND target.device_type=temp.device_type AND target.file_name=temp.file_name
        "
    ).whenNotMatchedInsertAll()\
        .whenMatchedUpdateAll()\
        .execute()
    '''
    try:
        current_files_temp_df.write\
        .mode("append")\
            .saveAsTable(bronze_table)
        print(f"Records successfully added in delta bronze with {len(files_to_load)} files..")

        # add file_load status
        for file in files_to_load:
            spark.sql(
                    '''
                    INSERT INTO IDENTIFIER(:target)
                    VALUES( :file_name, current_timestamp(), 'processed')
                    ''',
                    args = {
                        "target": file_load_table,
                        "file_name": file
                        }
                    )
        print("Added file load status")

    except Exception as e:
        print(f"Error happened... adding file status: {e}")
        for file in files_to_load:
            spark.sql('''
                    INSERT INTO IDENTIFIER(:target)
                    VALUES(:file_name, current_timestamp(), :status)
                    ''',
                    args = {
                        "target":file_load_table,
                        "file_name": file,
                        "status": "failed_to_load"
                        }
                    )


if __name__ == "__main__":
    main()








