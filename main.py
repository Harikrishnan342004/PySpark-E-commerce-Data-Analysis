from src.data_loader import create_spark, load_raw_data
from src.data_cleaning import clean_customer_data, clean_sales_data
from src.feature_engineering import create_user_item_matrix
from src.recommendation_model import train_model, generate_recommendations


def main():

    spark = create_spark()

    customer_df, sales_df = load_raw_data(spark)

    customer_clean = clean_customer_data(customer_df)

    sales_clean = clean_sales_data(sales_df)

    user_item_df = create_user_item_matrix(sales_clean)

    model = train_model(user_item_df)

    recommendations = generate_recommendations(
        model,
        user_id=10,
        top_n=5
    )

    recommendations.show(truncate=False)


if __name__ == "__main__":
    main()