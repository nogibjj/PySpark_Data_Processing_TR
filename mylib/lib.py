"""
library functions
"""
import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pyspark.sql.types import (
     StructType, 
     StructField, 
     IntegerType, 
     StringType, 
     FloatType
)

LOG_FILE = "pyspark_output.md"


def log_output(operation, output, query=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query: 
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")

def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    spark.stop()
    return "stopped spark session"

def extract(
    url="""
    https://github.com/fivethirtyeight/data/blob/master/alcohol-consumption/drinks.csv?raw=true 
    """,
    file_path="data/alcohol.csv",
    directory="data",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path
 

def load_data(spark, data="data/alcohol.csv", name="Alcohol"):
    """load data"""
    # data preprocessing by setting schema
    schema = StructType([
        StructField("country", StringType(), True),
        StructField("beer_servings", IntegerType(), True),
        StructField("spirit_servings", IntegerType(), True),
        StructField("wine_servings", IntegerType(), True),
        StructField("total_litres_of_pure_alcohol", FloatType(), True),
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)

    log_output("load data", df.limit(10).toPandas().to_markdown())

    return df


def query(spark, df, query, name): 
    """queries using spark sql"""
    df = df.createOrReplaceTempView(name)

    log_output("query data", spark.sql(query).toPandas().to_markdown(), query)

    return spark.sql(query).show()

def describe(df):
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("describe data", summary_stats_str)

    return df.describe().show()

def example_transform(df):
    """Do an example transformation on the "Alcohol" dataset"""
    conditions = [
        (col("beer_servings") > 10) | (col("spirit_servings") > 10),
        (col("wine_servings") > 10),
    ]

    categories = ["High Beer or Spirit", "High Wine"]

    df = df.withColumn("Alcohol_Category", when(
        conditions[0], categories[0]
        ).when(conditions[1], categories[1]).otherwise("Other"))

    # Log the first 10 rows of the transformed data
    log_output("transform data", df.limit(10).toPandas().to_markdown())

    return df.show()