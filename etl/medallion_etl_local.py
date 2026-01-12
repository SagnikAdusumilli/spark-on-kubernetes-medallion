from pyspark.sql import SparkSession

from pyspark.sql.functions import (col, to_date, trim, sum as fsum, avg as favg, count as fcount)

def build_spark():
    return (
    SparkSession.builder.appName("MedallionETL")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
    )

def main():
    spark = build_spark()

    #Raw Data Path
    raw_data_path = "file:/opt/app/data"

    # Outputs (when running in local mode)
    bronze_path = "file:/opt/app/outputs/bronze/sales"
    silver_path = "file:/opt/app/outputs/silver/sales"
    gold_path   = "file:/opt/app/outputs/gold/sales_agg"


    sales_df_raw = spark.read.option("header", "true").csv(f"{raw_data_path}/sales.csv")

    sales_df_raw.write.format("delta").mode("overwrite").save(f"{bronze_path}/raw_sales")


    df_silver = (
        sales_df_raw
        .withColumn("customer_name", trim(col("customer_name")))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("discount_pct", col("discount_pct").cast("double"))
        .withColumn("order_ts", col("order_ts").cast("timestamp"))
        .withColumn("order_date", to_date(col("order_ts")))
        .withColumn(
            "net_amount",
            (col("quantity") * col("unit_price") * (1 - col("discount_pct")))
        )
        # filters
        .filter(col("customer_name").isNotNull() & (trim(col("customer_name")) != ""))
        .filter(col("quantity").isNotNull() & (col("quantity") > 0))
        .filter(col("unit_price").isNotNull() & (col("unit_price") > 0))
        .filter(col("discount_pct").isNotNull() & 
        (col("discount_pct") >= 0) & (col("discount_pct") <= 1))
        .filter(col("order_ts").isNotNull())
    )

    df_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/cleaned_sales")

    df_gold  = (

        df_silver.filter(col("status") == "COMPLETED")
        .groupBy("order_date", "region", "customer_id", "customer_name")
        .agg(
            fsum(col("net_amount")).alias("total_sales"),
            fcount(col("order_id")).alias("order_count")
        )
        .orderBy(col("total_sales").desc())
    )

    df_gold.write.format("delta").mode("overwrite").save(f"{gold_path}/sales_aggregation")

    spark.stop()

if __name__ == "__main__":
    main()
