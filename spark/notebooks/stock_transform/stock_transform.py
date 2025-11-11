import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, round as spark_round

def main():
    bucket = os.getenv("S3_BUCKET", "stock-market")
    symbol = os.getenv("STOCK_SYMBOL")          # ✅ single symbol
    endpoint = os.getenv("ENDPOINT", "http://127.0.0.1:9000")

    if not symbol:
        print("No STOCK_SYMBOL provided, exiting.")
        return

    spark = (
        SparkSession.builder
        .appName(f"stock_transform_{symbol}")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    print(f"Processing: {symbol}")

    raw_path = f"s3a://{bucket}/{symbol}/*/prices.json"
    print(f"Reading: {raw_path}")

    df = spark.read.option("multiLine", True).json(raw_path)

    if df.rdd.isEmpty():
        print(f"No data found for {symbol}")
        spark.stop()
        return

    df_flat = (
        df
        .withColumn("timestamp", explode(col("timestamp")))
        .withColumn("quote", explode(col("indicators.quote")))
        .select(
            col("timestamp"),
            col("meta.symbol").alias("symbol"),
            spark_round(col("quote.open").getItem(0), 2).alias("open"),
            spark_round(col("quote.close").getItem(0), 2).alias("close"),
            spark_round(col("quote.high").getItem(0), 2).alias("high"),
            spark_round(col("quote.low").getItem(0), 2).alias("low"),
            col("quote.volume").getItem(0).alias("volume"),
        )
    )

    output = f"s3a://{bucket}/processed/{symbol}"
    print(f"Writing → {output}")

    df_flat.write.mode("overwrite").parquet(output)

    spark.stop()
    print(f"✅ Finished {symbol}!")


if __name__ == "__main__":
    main()
