import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._
import io.delta.tables._

object Consumer {
  def main(args: Array[String]): Unit = {
    val checkpointDir = "hdfs://localhost:19000/user/sparkStreaming/checkpoint"
    val resultPath = "hdfs://localhost:19000/user/sparkStreaming/output"
    val monthlyCountPath = "hdfs://localhost:19000/user/sparkStreaming/monthlyCount"
    val globalStatsPath = "hdfs://localhost:19000/user/sparkStreaming/globalStats"
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


    val stopWords = spark.read.textFile("src/main/resources/stopwords-fr.txt").collect().toSet

    // Extract and count words in messages
    val words = messageDF
      .select($"Date", explode(split(regexp_replace(col("Message"), "'", " "), "\\s+")).as("word"))
      .withColumn("word", regexp_replace($"word", "[^\\w]+", ""))
      .withColumn("word", lower($"word"))
      .filter($"word" =!= "" && !$"word".isin(stopWords.toSeq: _*)) // Filtrer les mots vides et les stop words
      .groupBy("word")
      .count()

    // Count messages by month
    val monthlyCounts = messageDF
      .withColumn("Month", date_format(col("Date"), "yyyy-MM"))
      .groupBy("Month")
      .count()

    // Count total messages and attachments
    val globalStats = messageDF
      .select("ServerID", "Message", "Attachments")
      .distinct() // Filtrer les doublons potentiels
      .groupBy("ServerID")
      .agg(
        count("Message").as("totalMessages"),
        count(expr("CASE WHEN Attachments IS NOT NULL AND Attachments != '' THEN 1 END")).as("totalAttachments")
      )

    // Count messages by user
    val userStats = messageDF
      .groupBy("UserID")
      .agg(
        count("Message").as("totalMessages")
      )

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

    val globalStatsQuery = globalStats.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!DeltaTable.isDeltaTable(spark, globalStatsPath)) {
          batchDF.write.format("delta").mode("overwrite").save(globalStatsPath)
        } else {
          val deltaTable = DeltaTable.forPath(spark, globalStatsPath)

          deltaTable.alias("existing")
            .merge(
              batchDF.alias("updates"),
              "existing.ServerID = updates.ServerID"
            )
            .whenMatched
            .updateExpr(
              Map(
                "totalMessages" -> "existing.totalMessages + updates.totalMessages",
                "totalAttachments" -> "existing.totalAttachments + updates.totalAttachments"
              )
            )
            .whenNotMatched
            .insertExpr(
              Map(
                "ServerID" -> "updates.ServerID",
                "totalMessages" -> "updates.totalMessages",
                "totalAttachments" -> "updates.totalAttachments"
              )
            )
            .execute()
        }
      }
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
                "totalMessages" -> "existing.totalMessages + updates.totalMessages"
              )
            )
            .whenNotMatched
            .insertExpr(
              Map(
                "UserID" -> "updates.UserID",
                "totalMessages" -> "updates.totalMessages",
              )
            )
            .execute()
        }
      }
      .option("checkpointLocation", checkpointDir + "_user")
      .start()


    wordCountQuery.awaitTermination()
    monthlyCountQuery.awaitTermination()
    globalStatsQuery.awaitTermination()
    userStatsQuery.awaitTermination()
  }
}