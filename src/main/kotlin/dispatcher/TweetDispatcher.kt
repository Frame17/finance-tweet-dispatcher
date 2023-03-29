package dispatcher

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.jetbrains.kotlinx.spark.api.forEach
import org.jetbrains.kotlinx.spark.api.forEachBatch
import java.io.Serializable
import java.util.*


private val kafkaProducer = Properties().let { props ->
    props["bootstrap.servers"] = "localhost:9093" // Kafka broker address
    props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"

    KafkaProducer<String, String>(props)
}

class TweetDispatcher(
    private val tweetPreprocessor: PipelineModel,
    private val tweetClassifier: NaiveBayesModel
): Serializable {

    fun dispatchToTopics(tweets: Dataset<Row>) = tweets
        .run(tweetPreprocessor::transform)
        .run(tweetClassifier::transform)
        .writeStream()
        .forEachBatch { batch, _ ->
            batch.forEach {
                val topic = "financial-tweets-${it.getAs<Int>("prediction").toInt()}"
                val record = ProducerRecord(topic, UUID.randomUUID().toString(), it.getAs<String>("tweet"))
                kafkaProducer.send(record)
            }
        }.start()
        .awaitTermination()
}