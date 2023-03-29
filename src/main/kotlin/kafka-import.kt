import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.BufferedReader
import java.io.FileReader
import java.util.*


private val kafkaProducer = Properties().let { props ->
    props["bootstrap.servers"] = "localhost:9093" // Kafka broker address
    props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"

    KafkaProducer<String, String>(props)
}

// todo - simulate event sending
fun main() {
    val csvFilePath = "valid_data.csv"
    val topic = "financial-tweets"

    BufferedReader(FileReader(csvFilePath)).use { br ->
        br.readLine()
        br.lines().forEach { line ->
            val text = line.removeRange(line.length - 2, line.length).removeSurrounding("\"")
            val target = line.removeRange(0, line.length - 1)
            target.toInt()
            val json = """{ "tweet": "$text" }""".trimMargin()
            val record = ProducerRecord(topic, UUID.randomUUID().toString(), json)
            kafkaProducer.send(record)
        }
    }

    kafkaProducer.flush()
    kafkaProducer.close()
}