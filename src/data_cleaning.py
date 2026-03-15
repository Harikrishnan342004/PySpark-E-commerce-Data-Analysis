from pyspark.sql.functions import col


def clean_customer_data(customer_df):

    customer_df = customer_df.dropDuplicates()

    customer_df = customer_df.dropna(subset=["customer_id"])

    customer_df = customer_df.filter(
        (col("age") > 0) & (col("age") < 100)
    )

    return customer_df


def clean_sales_data(sales_df):

    sales_df = sales_df.dropDuplicates()

    sales_df = sales_df.dropna(
        subset=["customer_id", "product_id"]
    )

    sales_df = sales_df.filter(col("price") > 0)

    sales_df = sales_df.filter(
        (col("rating") >= 1) & (col("rating") <= 5)
    )

    return sales_df