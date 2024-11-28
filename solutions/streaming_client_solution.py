import sys
import tempfile
import time
sys.path.append("..")
                
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import IntegerType, StructType, StructField

# Create local StreamingContext with a batch interval of 10s
spark = (SparkSession.builder
           .appName("DStream Client")
           .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


jsonSchema = StructType([ StructField("females", IntegerType(), True), StructField("males", IntegerType(), True),  StructField("age", IntegerType(), True) ,  StructField("year", IntegerType(), True)])

stream = spark.readStream.format('socket').option('host', 'localhost').option('port', 9999).load()

print(stream.isStreaming )  # Returns True for DataFrames that have streaming sources

stream.printSchema()
# string to json to columns and filter records <= 10000 
stream = stream.select(from_json(stream.value,jsonSchema).alias('data'))\
               .select(col('data.*')).where("(males+females) > 10000")


stream.writeStream.format('console').start().awaitTermination()

# Parquet file generation
""" with tempfile.TemporaryDirectory() as cp:
   writer = stream.writeStream.format("parquet")\
                 .option("path", "data/census_2010.parquet")\
                 .outputMode("append")\
                 .option("checkpointLocation", cp)\
                 .start()
   time.sleep(50)
   writer.stop() """



# CSV file generation 
""" with tempfile.TemporaryDirectory() as file, tempfile.TemporaryDirectory() as cp:
    writer = stream.writeStream.format("csv")\
                    .option("checkpointLocation", cp) \
                    .start(file)
    time.sleep(50)
    writer.stop()
    spark.read.csv(file).toPandas().to_csv("data/census_2010.csv",index=False) # Save the data into a single file to a permanent location 
 """


spark.stop()


