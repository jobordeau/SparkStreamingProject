import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._
import io.delta.tables._

object Consumer {
  def main(args: Array[String]): Unit = {
    val checkpointDir = "hdfs://localhost:19000/user/sparkStreaming/checkpoint"
    val resultPath = "hdfs://localhost:19000/user/sparkStreaming/output"

    val spark = SparkSession.builder
      .appName("StructuredKafkaWordCount")
      .master("local[4]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 3)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._

    val messageSchema = new StructType()
      .add("Date", TimestampType)
      .add("Channel", StringType)
      .add("ServerID", StringType)
      .add("ServerName", StringType)
      .add("UserID", StringType)
      .add("Message", StringType)
      .add("Attachments", StringType)

    val kafkaBootstrapServers = "localhost:9092"
    val kafkaTopic = "sparkStreaming"

    // Read messages from Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .load()

    // Convert messages to DataFrame using the defined schema
    val messages = df.selectExpr("CAST(value AS STRING)").as[String]
    val messageDF = messages.select(from_json(col("value"), messageSchema).as("data")).select("data.*")

    // Extract and count words in messages
    val words = messageDF
      .select($"Date", explode(split(col("Message"), "\\s+")).as("word"))
      .filter(col("word") =!= "")  // Filter out empty words
      .groupBy("word")
      .count()

    // Use foreachBatch for incremental updates
    val query = words.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // Check if the Delta table exists
        if (!DeltaTable.isDeltaTable(spark, resultPath)) {
          batchDF.write.format("delta").mode("overwrite").save(resultPath)
        } else {
          // Load the existing Delta table
          val deltaTable = DeltaTable.forPath(spark, resultPath)

          // Perform a merge for incremental updates
          deltaTable.as("t")
            .merge(
              batchDF.as("s"),
              "t.word = s.word"
            )
            .whenMatched
            .updateAll()
            .whenNotMatched
            .insertAll()
            .execute()
        }
      }
      .option("checkpointLocation", checkpointDir)
      .start()

    query.awaitTermination()
  }
}
