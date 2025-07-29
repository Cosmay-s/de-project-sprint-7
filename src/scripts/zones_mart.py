import sys
import pyspark.sql.functions as F
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from timezone import add_coords_timezone


def main():
    date = sys.argv[1]
    days_count = int(sys.argv[2])
    events_base_path = sys.argv[3]
    geo_base_path = sys.argv[4]
    zones_mart_path = sys.argv[5]

    conf = SparkConf().setAppName(f"ZonesMart-{date}-d{days_count}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    end_date = F.to_date(F.lit(date), "yyyy-MM-dd")

    events_zones_mart = (
        sql.read.parquet(events_base_path)
        .filter(F.col("date").between(F.date_sub(end_date, days_count),
                                      end_date))
    )

    geo = (
        sql.read.option("delimiter", ",")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(geo_base_path)
    )

    message_city_df = add_coords_timezone(events_zones_mart, geo)

    zones_mart = calculate_zones_mart(message_city_df)

    zones_mart.write.mode("overwrite").parquet(f"{zones_mart_path}/date={date}/depth={days_count}")


def calculate_zones_mart(message_city_df):
    df = (
        message_city_df
        .withColumn("month", F.date_format("date", "yyyy-MM"))
        .withColumn(
            "week",
            F.concat_ws(
                "-",
                F.date_format("date", "yyyy"),
                F.format_string("%02d", F.weekofyear("date"))
            )
        )
    )

    df = df.select("user_id", "event_type", "city", "month", "week")

    month_agg = (
        df.groupBy("month", "city")
        .agg(
            F.sum(F.when(F.col("event_type") == "message",
                         1).otherwise(0)).alias("month_message"),
            F.sum(F.when(F.col("event_type") == "reaction",
                         1).otherwise(0)).alias("month_reaction"),
            F.sum(F.when(F.col("event_type") == "subscription",
                         1).otherwise(0)).alias("month_subscription"),
            F.countDistinct(F.when(F.col("event_type") == "registration",
                                   F.col("user_id"))).alias("month_user")
        )
        .withColumnRenamed("city", "zone_id")
        .withColumnRenamed("month", "period")
        .withColumn("period_type", F.lit("month"))
    )

    week_agg = (
        df.groupBy("week", "city")
        .agg(
            F.sum(F.when(F.col("event_type") == "message",
                         1).otherwise(0)).alias("week_message"),
            F.sum(F.when(F.col("event_type") == "reaction",
                         1).otherwise(0)).alias("week_reaction"),
            F.sum(F.when(F.col("event_type") == "subscription",
                         1).otherwise(0)).alias("week_subscription"),
            F.countDistinct(F.when(F.col("event_type") == "registration",
                                   F.col("user_id"))).alias("week_user")
        )
        .withColumnRenamed("city", "zone_id")
        .withColumnRenamed("week", "period")
        .withColumn("period_type", F.lit("week"))
    )

    zones_mart = month_agg.select(
        "period_type", "period", "zone_id",
        "month_message", "month_reaction", "month_subscription", "month_user"
    ).join(
        week_agg.select(
            "period", "zone_id",
            "week_message", "week_reaction", "week_subscription", "week_user"
        ),
        on=["period", "zone_id"],
        how="outer"
    )

    zones_mart = zones_mart.fillna(0, subset=[
        "month_message", "month_reaction", "month_subscription", "month_user",
        "week_message", "week_reaction", "week_subscription", "week_user"
    ])

    return zones_mart


if __name__ == "__main__":
    main()
