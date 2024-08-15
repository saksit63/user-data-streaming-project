from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

def main():  
    spark = SparkSession.builder\
        .appName("Streamming")\
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "mysql:mysql-connector-java:8.0.33,")\
        .getOrCreate()

    # กำหนด schema ของข้อมูลใน topic
    schema = StructType([
        StructField("payload", StructType([
            StructField("_id", StringType(), False),
            StructField("firstname", StringType(), True),
            StructField("lastname", StringType(), True),
            StructField("email", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("__op", StringType(), False)
        ]), True)
    ])

    # อ่านข้อมูลจาก Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("subscribe", "mongodb.source.users") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # แปลง value จาก Kafka topic ให้เป็น String
    value_df = df.selectExpr("CAST(value AS STRING)")

    # แปลง JSON string เป็น DataFrame ด้วย schema ที่กำหนด
    final_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    # จัดรูปแบบ df
    stream_df = final_df.select(
        col("payload._id").alias("user_id"),
        col("payload.firstname").alias("first_name"),
        col("payload.lastname").alias("last_name"),
        col("payload.email").alias("email"),
        col("payload.age").alias("age"),
        col("payload.__op").alias("status_op")
    ).withColumn(
        "operation",
        when(col("status_op") == "c", "create")
        .when(col("status_op") == "r", "create")
        .when(col("status_op") == "u", "update")
        .otherwise("unknown")
    ).select("user_id", "first_name", "last_name", "email", "age", "operation")

    # ฟังก์ชันในการเขียนข้อมูลลง mysql
    def write_to_mysql(df, epoch_id, table_name):
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://mysql:3306/sink") \
            .option("dbtable", table_name) \
            .option("user", "root") \
            .option("password", "1234") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()
        
    # เขียนข้อมูลลงในฐานข้อมูล MySQL โดยใช้ฟังก์ชัน foreachBatch
    query = stream_df.writeStream.foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, "users")).start()

    # รอจนกว่า stream จะเสร็จสิ้น
    query.awaitTermination()


if __name__ == "__main__":
    main()    

