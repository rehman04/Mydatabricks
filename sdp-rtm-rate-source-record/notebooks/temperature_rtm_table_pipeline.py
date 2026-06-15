from pyspark import pipelines as dp
from pyspark.sql.functions import avg, col, count, expr, max as max_, min as min_, window

print("Loading pipeline library: hot_temperatures")

@dp.table(
    name="hot_temperatures",
    comment="Windowed temperature aggregates from a rate source",
)
def hot_temperatures():
    print("Building streaming table hot_temperatures")
    return (
        spark.readStream.format("rate")
        .option("rowsPerSecond", "1")
        .load()
        .select(
            col("timestamp").alias("source_timestamp"),
            expr("19 + rand() * 7").alias("temperature_c"),
        )
        .withWatermark("source_timestamp", "10 seconds")
        .groupBy(window(col("source_timestamp"), "10 seconds", "2 seconds"))
        .agg(
            count("*").alias("event_count"),
            avg("temperature_c").alias("avg_temp_c"),
            min_("temperature_c").alias("min_temp_c"),
            max_("temperature_c").alias("max_temp_c"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_count"),
            col("avg_temp_c"),
            col("min_temp_c"),
            col("max_temp_c"),
        )
    )
