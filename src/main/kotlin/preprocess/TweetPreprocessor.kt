package preprocess

import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.jetbrains.kotlinx.spark.api.col

class TweetPreprocessor : (Dataset<Row>) -> Dataset<Row> {

    override fun invoke(dataset: Dataset<Row>): Dataset<Row> {
        val tokenizer = RegexTokenizer()
            .setInputCol("tweet")
            .setOutputCol("tokens")
            .setPattern("([’']s)?[,!?\\\"'`:;.—\\-“”‘’]*\\s+[,!?\\\"'`:;.—\\-“”‘’]*")
            .setMinTokenLength(2)

        val stopWords = StopWordsRemover().setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
            .setInputCol("tokens")
            .setOutputCol("tokens_clean")

        val hashingTF = HashingTF()
            .setInputCol("tokens_clean")
            .setOutputCol("tf")

        val idf = IDF()
            .setInputCol("tf")
            .setOutputCol("features")

        return dataset.filter(col("class").isNotNull)
            .run(tokenizer::transform)

            .run(stopWords::transform)
            .run(hashingTF::transform)
            .let {
                idf.fit(it).transform(it)
            }
    }
}