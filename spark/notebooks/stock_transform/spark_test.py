

from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    def app():
        spark = SparkSession.builder.appName("FormatCrypto") \
            .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
            .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")) \
            .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://host.docker.internal:9000")) \
            .config("fs.s3a.connection.ssl.enabled", "false") \
            .config("fs.s3a.path.style.access", "true") \
            .config("fs.s3a.attempts.maximum", "1") \
            .config("fs.s3a.connection.establish.timeout", "5000") \
            .config("fs.s3a.connection.timeout", "10000") \
            .getOrCreate()

        try:

            df = spark.read\
                .json("s3a://cryptodataproject/bronze/coin_gecko_data.json")

            # Transform
            df_transformed = df.select("id", "symbol","name","current_price","total_volume")


            df_transformed.write.mode("overwrite") \
                .option("header", "true") \
                .csv("s3a://sparktest/transformed/")

            print("ETL job completed successfully")

        except Exception as e:
            print(f"Error in ETL job: {str(e)}")
            raise
    app()

