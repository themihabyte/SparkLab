from pyspark.sql import SparkSession
import pyspark.sql.functions as F


MONGO_DB = "mongodb://router01:27017/?authSource=admin"
COLLECTION = "london.taxi_rides"


def init_spark():
    spark = SparkSession.builder.appName("highload-lab5").getOrCreate()
    return spark


def extract(spark):
    df = (
        spark.read.format("mongodb")
        .option(
            "spark.mongodb.read.connection.uri",
            MONGO_DB,
        )
        .option("spark.mongodb.read.database", COLLECTION.split(".")[0])
        .option("spark.mongodb.read.collection", COLLECTION.split(".")[1])
        .load()
    )
    return df


def quality_check(spark, df):
    df.createOrReplaceTempView("df")
    assert df.select(F.countDistinct("driver_id").alias("nunique")).first().nunique > 2000
    assert df.select(F.countDistinct("client_id").alias("nunique")).first().nunique > 4000
    assert spark.sql("SELECT ALL(start_date < end_date) as res FROM df").first().res
    assert spark.sql("SELECT ANY(driver_review.rating IS NOT NULL) as res FROM df").first().res
    assert spark.sql("SELECT ANY(driver_review.categories IS NOT NULL) as res FROM df").first().res
    assert spark.sql("SELECT ANY(driver_review.text IS NOT NULL) as res FROM df").first().res
    assert spark.sql("SELECT ANY(client_review.rating IS NOT NULL) as res FROM df").first().res
    assert spark.sql("SELECT ANY(client_review.categories IS NOT NULL) as res FROM df").first().res


def transform(raw_data):
    result = (
        raw_data.select(F.col("driver_review.text").alias("review_text"))
        .filter("review_text is not null")
        .distinct()
        .select("review_text", F.length("review_text").alias("length"))
        .sort("length", ascending=False)
        .limit(10)
    )
    return result


def load(spark, data):
    data.show(100)


def main():
    spark = init_spark()

    raw_data = extract(spark)
    quality_check(spark, raw_data)
    data = transform(raw_data)
    load(spark, data)

    spark.stop()


if __name__ == "__main__":
    main()
