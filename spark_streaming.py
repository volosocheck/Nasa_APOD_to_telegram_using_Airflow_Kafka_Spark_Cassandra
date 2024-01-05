import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

def create_spark_session():

    try:
        spark = SparkSession \
            .builder \
            .appName("CurrencyRatesStreaming") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info('Spark session created successfully')
    except Exception as e:
        logger.error(f"Couldn't create the spark session: {e}")

    return spark

def create_initial_dataframe(spark_session):

    try:
        df = spark_session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
            .option("subscribe", "nasa_apod") \
            .option("startingOffsets", "earliest") \
            .load()

        df.printSchema()
        logger.info("Initial dataframe created successfully")
    except Exception as e:
        logger.warning(f"Initial dataframe couldn't be created due to exception: {e}")

    return df

def create_final_dataframe(df):
    """
    Transforms the initial DataFrame into the final DataFrame.
    """
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("explanation", StringType(), True),
        StructField("url", StringType(), True),
        StructField("hdurl", StringType(), True),
        StructField("date", StringType(), True)
    ])

    df = df.selectExpr("CAST(value AS STRING)") \
          .select(from_json(col("value"), schema).alias("data")) \
          .select("data.title", "data.explanation", "data.url", "data.hdurl", "data.date")
    return df

def start_streaming(df):

    query = (df.writeStream
             .format("org.apache.spark.sql.cassandra")
             .outputMode("append")
             .option("keyspace", "nasa")
             .option("table", "nasa_apod")
             .option("checkpointLocation", "/opt/bitnami/spark/checkpoint")
             .start())
    return query.awaitTermination()

def write_streaming_data():
    spark = create_spark_session()
    df_initial = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df_initial)
    start_streaming(df_final)

if __name__ == '__main__':
    write_streaming_data()