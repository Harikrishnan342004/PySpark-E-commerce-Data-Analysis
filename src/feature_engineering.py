from pyspark.sql.functions import col


def create_user_item_matrix(sales_df):

    user_item_df = sales_df.select(
        col("customer_id").alias("userId"),
        col("product_id").alias("itemId"),
        col("rating")
    )

    return user_item_df