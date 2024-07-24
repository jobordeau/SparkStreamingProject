import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._
import io.delta.tables._

object Consumer {
  def main(args: Array[String]): Unit = {
    val checkpointDir = "hdfs://localhost:19000/user/sparkStreaming/checkpoint"
    val resultPath = "hdfs://localhost:19000/user/sparkStreaming/output"
    val monthlyCountPath = "hdfs://localhost:19000/user/sparkStreaming/monthlyCount"
    val userStatsPath = "hdfs://localhost:19000/user/sparkStreaming/userStats"

    val spark = SparkSession.builder
      .appName("StructuredKafkaWordCount")
      .master("local[*]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 200)
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

    // Apply watermark to handle late data
    val watermarkedDF = messageDF.withWatermark("Date", "10 minutes")

    val stopWords = spark.read.textFile("src/main/resources/stopwords-fr.txt").collect().toSet

    // Extract and count words in messages
    val words = watermarkedDF
      .select($"Date", explode(split(regexp_replace(col("Message"), "'", " "), "\\s+")).as("word"))
      .withColumn("word", regexp_replace($"word", "[^\\w]+", ""))
      .withColumn("word", lower($"word"))
      .filter($"word" =!= "" && !$"word".isin(stopWords.toSeq: _*)) // Filtrer les mots vides et les stop words
      .groupBy("word")
      .count()

    // Count messages by month
    val monthlyCounts = watermarkedDF
      .withColumn("Month", date_format(col("Date"), "yyyy-MM"))
      .groupBy("Month")
      .count()


    // Count messages by user
    val userStats = watermarkedDF
      .withColumn("UserID", col("UserID"))
      .groupBy("UserID")
      .count()

    // Use foreachBatch for incremental updates
    val wordCountQuery = words.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!DeltaTable.isDeltaTable(spark, resultPath)) {
          batchDF.write.format("delta").mode("overwrite").save(resultPath)
        } else {
          val deltaTable = DeltaTable.forPath(spark, resultPath)
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

    val monthlyCountQuery = monthlyCounts.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!DeltaTable.isDeltaTable(spark, monthlyCountPath)) {
          batchDF.write.format("delta").mode("overwrite").save(monthlyCountPath)
        } else {
          val deltaTable = DeltaTable.forPath(spark, monthlyCountPath)
          deltaTable.as("t")
            .merge(
              batchDF.as("s"),
              "t.Month = s.Month"
            )
            .whenMatched
            .updateAll()
            .whenNotMatched
            .insertAll()
            .execute()
        }
      }
      .option("checkpointLocation", checkpointDir + "_monthly")
      .start()



    val userStatsQuery = userStats.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!DeltaTable.isDeltaTable(spark, userStatsPath)) {
          batchDF.write.format("delta").mode("overwrite").save(userStatsPath)
        } else {
          val deltaTable = DeltaTable.forPath(spark, userStatsPath)
          deltaTable.as("existing")
            .merge(
              batchDF.as("updates"),
              "existing.UserID = updates.UserID"
            )
            .whenMatched
            .updateExpr(
              Map(
                "count" -> "existing.count + updates.count"
              )
            )
            .whenNotMatched
            .insertExpr(
              Map(
                "UserID" -> "updates.UserID",
                "count" -> "updates.count",
              )
            )
            .execute()
        }
      }
      .option("checkpointLocation", checkpointDir + "_user")
      .start()

    wordCountQuery.awaitTermination()
    monthlyCountQuery.awaitTermination()
    userStatsQuery.awaitTermination()
  }
}
