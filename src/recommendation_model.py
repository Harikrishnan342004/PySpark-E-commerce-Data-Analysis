from pyspark.ml.recommendation import ALS


def train_model(user_item_df):

    als = ALS(
        userCol="userId",
        itemCol="itemId",
        ratingCol="rating",
        coldStartStrategy="drop"
    )

    model = als.fit(user_item_df)

    return model


def generate_recommendations(model, user_id, top_n):

    user_df = model._spark.createDataFrame(
        [(user_id,)], ["userId"]
    )

    recommendations = model.recommendForUserSubset(
        user_df, top_n
    )

    return recommendations