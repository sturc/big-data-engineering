# %% [markdown]
# # Build Features on the Sales Dataset in Spark

# %%
import sys
sys.path.append("..")
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number   
from pyspark.ml.feature import StringIndexer, VectorAssembler

# %% [markdown]
# ## Select the churn file 

# %%
inputFile = "hdfs:///data/sales.csv"

# %% [markdown]
# ## Create the Spark Session 

# %%
#create a SparkSession
spark = SparkSession \
       .builder \
       .master("yarn") \
       .appName("SalesFeatureEngineering") \
       .getOrCreate()
# create a DataFrame using an inferred Schema 
df = spark.read.option("header", "true") \
       .option("inferSchema", "true") \
       .option("delimiter", ",") \
       .csv(inputFile)

# %% [markdown]
# ## Data Preparation
# ### Build new features from existing ones

# %%
df.printSchema()

df_window = Window.orderBy(col("sales").desc())
result_df = df.withColumn("salesrank", row_number().over(df_window))
result_df.show()
division_indexer = StringIndexer().setInputCol("division").setOutputCol("division_num").fit(result_df)
education_indexer = StringIndexer().setInputCol("level of education").setOutputCol("education_num").fit(result_df)


# %% [markdown]
#  ### Build the feature vector

# %%
featureCols = result_df.columns.copy()
featureCols.remove("division")
featureCols.remove("level of education")
featureCols.remove("sales")
print(featureCols)

# %% [markdown]
# ### Build the feature Vector Assembler

# %%
assembler =  VectorAssembler(outputCol="features", inputCols=list(featureCols))

# %% [markdown]
# ## Do the Data Preparation

# %%

indexed_data = education_indexer.transform(division_indexer.transform(result_df))
labeledPointData = assembler.transform(indexed_data)
labeledPointData.show()


# %%
spark.stop()


