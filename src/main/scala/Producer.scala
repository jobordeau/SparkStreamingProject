import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json._
import com.opencsv.{CSVReader, CSVParserBuilder, CSVReaderBuilder}
import java.io.FileReader
import java.util.Properties
import scala.collection.JavaConverters._

object Producer extends App {
  case class Message(Date: String, Channel: String, ServerID: String, ServerName: String, UserID: String, Message: String, Attachments: String)

  implicit val messageWrites: Writes[Message] = Json.writes[Message]

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  def readAndSendMessages(filePath: String, batchSize: Int, topic: String): Unit = {
    val csvParser = new CSVParserBuilder().withSeparator(',').build()
    val reader = new CSVReaderBuilder(new FileReader(filePath))
      .withCSVParser(csvParser)
      .build()

    try {
      println("Lecture du dataset...")

      // Lire et ignorer l'en-tête
      reader.readNext()

      var lines = reader.readNext()
      var batch = List[Array[String]]()

      while (lines != null) {
        if (lines.length == 7) {
          batch = batch :+ lines
        } else {
          println(s"Ligne incorrecte ignorée: ${lines.mkString(",")}")
        }

        if (batch.size >= batchSize) {
          sendBatch(batch, topic)
          batch = List[Array[String]]()
          producer.flush()
          Thread.sleep(1000)
        }

        lines = reader.readNext()
      }

      if (batch.nonEmpty) {
        sendBatch(batch, topic)
        producer.flush()
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      reader.close()
      producer.close()
    }
  }

  def sendBatch(batch: List[Array[String]], topic: String): Unit = {
    batch.foreach { cols =>
      val msgObj = Message(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6))
      val jsonString = Json.stringify(Json.toJson(msgObj))
      val record = new ProducerRecord[String, String](topic, jsonString)
      producer.send(record)
    }
  }

  // Appel de la fonction avec le chemin du fichier, la taille du batch et le topic Kafka
  readAndSendMessages("src/dataset/train.csv", batchSize = 100, topic = "sparkStreaming")
}