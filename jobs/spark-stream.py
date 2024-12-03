from pyspark.sql import SparkSession
from config import aws_credentials
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import from_json, col

def main():
    try:
        spark = (SparkSession.builder.appName("vehicleTracking")
                 .config("spark.jars.packages", 
                         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                         "org.apache.hadoop:hadoop-aws:3.3.4,"
                         "com.amazonaws:aws-java-sdk-s3:1.12.767")
                 .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                 .config("spark.hadoop.fs.s3a.access.key", aws_credentials.get("AWS_ACCESS_KEY"))
                 .config("spark.hadoop.fs.s3a.secret.key", aws_credentials.get("AWS_SECRET_KEY"))
                 .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                         "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                 .getOrCreate())
        print("Spark session created successfully.")
    except Exception as e:
        print(f"Error creating Spark session: {e}")

    # Adjust log level to minimize console output on executors
    spark.sparkContext.setLogLevel("WARN")

    # Define the schema for the various streams  
    vehicle_info_schema = StructType([
        StructField('vehicle_id', StringType(), True),
        StructField('type', StringType(), True),
        StructField('speed', IntegerType(), True),
        StructField('status', StringType(), True),
        StructField('timestamp', TimestampType(), True)
    ])

    gps_info_schema = StructType([
        StructField('vehicle_id', StringType(), True),
        StructField('latitude', DoubleType(), True),
        StructField('longitude', DoubleType(), True),
        StructField('timestamp', TimestampType(), True)
    ])

    camera_info_schema = StructType([
        StructField('vehicle_id', StringType(), True),
        StructField('frame_number', IntegerType(), True),  # Corrected this line to use IntegerType
        StructField('camera_status', StringType(), True),  # Fixed field name (was duplicated)
        StructField('timestamp', TimestampType(), True)
    ])

    weather_info_schema = StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("location", StringType(), True),      
        StructField("temperature", IntegerType(), True),  
        StructField("humidity", IntegerType(), True),     
        StructField("condition", StringType(), True),     
        StructField("timestamp", TimestampType(), True)
    ])

    emergency_info_schema = StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("emergency_type", StringType(), True),  
        StructField("severity", IntegerType(), True),       
        StructField("timestamp", TimestampType(), True)
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')  # Fixed typo from 'valueS' to 'value'
                .select(from_json(col('value'), schema).alias('data'))  # Ensure the schema is applied correctly
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
                )
    
    def streamWriter(df, checkpointFolder, output):
        return (df.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start()
                )

    # Reading the individual dataframes from the Kafka topics
    vehicle_df = read_kafka_topic('vehicle_info', vehicle_info_schema).alias('vehicle')
    gps_df = read_kafka_topic('gps_info', gps_info_schema).alias('gps')
    camera_df = read_kafka_topic('camera_info', camera_info_schema).alias('camera')
    weather_df = read_kafka_topic('weather_info', weather_info_schema).alias('weather')
    emergency_df = read_kafka_topic('emergency_info', emergency_info_schema).alias('emergency')

    # Writing the individual dataframes to S3 and collecting the queries
    queries = []
    queries.append(streamWriter(vehicle_df, 's3a://vehicle-tracking-bok1/checkpoints/vehicle_info',
                                's3a://vehicle-tracking-bok1/data/vehicle_info'))
    queries.append(streamWriter(gps_df, 's3a://vehicle-tracking-bok1/checkpoints/gps_info',
                                's3a://vehicle-tracking-bok1/data/gps_info'))
    queries.append(streamWriter(camera_df, 's3a://vehicle-tracking-bok1/checkpoints/camera_info',
                                's3a://vehicle-tracking-bok1/data/camera_info'))
    queries.append(streamWriter(weather_df, 's3a://vehicle-tracking-bok1/checkpoints/weather_info',
                                's3a://vehicle-tracking-bok1/data/weather_info'))
    queries.append(streamWriter(emergency_df, 's3a://vehicle-tracking-bok1/checkpoints/emergency_info',
                                's3a://vehicle-tracking-bok1/data/emergency_info'))

    # Await termination of all queries
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()

