from pyspark.sql import SparkSession


def create_spark():

    spark = SparkSession.builder \
        .appName("ProductRecommendationSystem") \
        .getOrCreate()

    return spark


def load_raw_data(spark):

    customer_df = spark.read.csv(
        "data/raw/customer_raw.csv",
        header=True,
        inferSchema=True
    )

    sales_df = spark.read.csv(
        "data/raw/sales_raw.csv",
        header=True,
        inferSchema=True
    )

    return customer_df, sales_df