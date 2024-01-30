import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, sha1, col, lit
from pyspark.sql import functions as F
from pyspark import SparkConf, SparkContext
import json

#import modules: configs, helpers
from helpers import helper_functions

#Define custom configuration properties
spark_conf = SparkConf()
spark_conf.set("spark.driver.extraClassPath", "/opt/spark/work-dir/postgresql-42.7.0.jar")

#Set spark context and start session
context = SparkContext(conf=spark_conf)
spark = (SparkSession(context)
         .builder
         .appName("DataPipeline")
         .getOrCreate())

def bronze(config):
    #Ingest all source data
    data_frames = []
    for source, meta_data in config["data_sources"].items():
        bronze_df = helper_functions.read_data(spark, meta_data["path"], meta_data["format"])
        data_frames.append(bronze_df)
        
    #Persist the data to bronze layer   
    index = 0
    for target, meta_data in config["storage_layer"]["bronze"].items():
        helper_functions.write_data(spark, data_frames[index], meta_data)
        index += 1
    
    return data_frames
    
def silver(datasets, config):
    #Client transform 
    clients_silver_df = (datasets[0]
                         .withColumn("email", sha1(col("email")))
                         .withColumn("address", sha1(col("address")))
                         .withColumn("financial_assessment", datasets[0]["financial_assessment"].cast("int"))
                        )
    #Collaterals transform
    collaterals_silver_df = (datasets[1]
                             .withColumnRenamed("ticker", "ticker")
                             .withColumnRenamed("type", "collateral_type")
                             .withColumnRenamed("value", "collateral_value")
                            )
    
    collaterals_silver_df = (collaterals_silver_df                            
                             .withColumn("client_id", collaterals_silver_df["client_id"].cast("int"))
                             .withColumn("collateral_value", collaterals_silver_df["collateral_value"].cast("int"))
                            )
    #Stocks transform
    stocks_silver_df = datasets[2].withColumnRenamed("price", "stock_price")
    
    datasets = [clients_silver_df, collaterals_silver_df, stocks_silver_df]
    
    #Persist the data to bronze layer   
    index = 0
    for target, meta_data in config["storage_layer"]["silver"].items():
        helper_functions.write_data(spark, datasets[index], meta_data)
        index += 1
    
    return datasets

def gold(datasets, config):
    #Perform agregations
    collateral_status_df = datasets[1].join(datasets[2], on=['ticker', 'ticker'], how='inner')
    collateral_status_df.show()
    
    collateral_status_df = collateral_status_df.drop("ticker", "collateral_type", "stock_price", "volume")
    collateral_status_df.show()
    
    # Select specific columns and a calculated column
    collateral_status_df = (collateral_status_df
        .select((concat(col("date"), lit(" "), col("time"))).alias("date_time"), "client_id", (F.col("collateral_value") * F.col("change")).alias("current_collateral_value"))
                           )
    
    #Write to gold storage layer    
    helper_functions.write_data(spark, collateral_status_df, config["storage_layer"]["gold"]["collateral_status"])

    (collateral_status_df.write
     .format(config["jdbc"]["format"])
     .option("url", config["jdbc"]["url"])
     .option("user", config["jdbc"]["user"])
     .option("password", config["jdbc"]["password"])
     .option("dbtable", config["jdbc"]["dbtable"])
     .mode(config["jdbc"]["mode"])
     .save())
    

def pipeline_run():
    # Read the configuration file as a dictionary
    config_file = "/opt/spark/work-dir/configs/configs.json"
    
    with open(config_file) as f:
        config = json.load(f)
    
    #Pipeline run for bronze silver and gold here    
    bronze_datasets = bronze(config)
    silver_datasets = silver(bronze_datasets, config)
    gold(silver_datasets, config)
    
pipeline_run()