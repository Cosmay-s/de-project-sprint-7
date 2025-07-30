import pyspark.sql.functions as F
from pyspark.sql.window import Window


def add_coords_timezone(events_df, geo_df):
    events_prepared = (
        events_df
        .withColumn("unique_id", F.monotonically_increasing_id())
        .select(
            "unique_id",
            "event.message_id",
            F.col("event.message_from").alias("user_id"),
            F.col("lat").alias("message_lat"),
            F.col("lon").alias("message_lon"),
            "date",
            F.to_timestamp("event.datetime").alias("datetime"),
        )
        .withColumn("lat1", F.radians(F.col("message_lat")))
        .withColumn("lon1", F.radians(F.col("message_lon")))
    )

    geo_prepared = (
        geo_df
        .select(
            "city",
            F.col("lat").alias("city_lat"),
            F.col("lon").alias("city_lon"),
            F.col("tz").alias("city_tz"),
        )
        .withColumn("lat2", F.radians(F.col("city_lat")))
        .withColumn("lon2", F.radians(F.col("city_lon")))
    )

    geo_broadcast = F.broadcast(geo_prepared)
    joined = events_prepared.crossJoin(geo_broadcast)

    R = 6371.0
    distance_expr = 2 * R * F.asin(
        F.sqrt(
            F.pow(F.sin((F.col("lat2") - F.col("lat1")) / 2), 2) +
            F.cos(F.col("lat1")) * F.cos(F.col("lat2")) *
            F.pow(F.sin((F.col("lon2") - F.col("lon1")) / 2), 2)
        )
    )

    joined_with_dist = joined.withColumn("distance", distance_expr)

    window = Window.partitionBy("unique_id").orderBy(F.col("distance"))

    message_city = (
        joined_with_dist
        .withColumn("rn", F.row_number().over(window))
        .filter("rn = 1")
        .drop("lat1", "lon1", "lat2", "lon2", "rn")
    )

    return message_city


def calculate_user_localtime(message_city_df):
    window = Window().partitionBy(["user_id"]).orderBy(F.desc("datetime"))
    local_time = (
        message_city_df.select("user_id", "datetime", "city_tz")
        .withColumn("rn", F.row_number().over(window))
        .where("rn=1")
        .withColumn(
            "local_time", F.from_utc_timestamp(F.col("datetime"),
                                               F.col("city_tz"))
        )
        .drop("datetime", "city_tz", "rn")
    )
    return local_time
