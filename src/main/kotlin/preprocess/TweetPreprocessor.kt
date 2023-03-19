package preprocess

import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataTypes
import org.jetbrains.kotlinx.spark.api.UDF1
import org.jetbrains.kotlinx.spark.api.col
import scala.collection.JavaConverters
import scala.collection.Seq
import java.io.Serializable

class TweetPreprocessor : (Dataset<Row>) -> Dataset<Row>, Serializable {

    override fun invoke(dataset: Dataset<Row>): Dataset<Row> {
        val tokenizer = RegexTokenizer()
            .setInputCol("tweet")
            .setOutputCol("tokens")
            .setPattern("([’']s)?[,!?\"'`:;.—-“”‘’]*\\s+[,!?\"'`:;.—-“”‘’]*")
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
            .withColumn("tokens_clean", filterLinksUDF.apply(col("tokens_clean")))
            .run(hashingTF::transform)
            .let {
                idf.fit(it).transform(it)
            }
            .drop("tweet", "tokens", "tokens_clean", "tf")
    }

    private val filterLinksUDF = udf(
        UDF1 { tokens: Seq<String> ->
            JavaConverters.asJavaCollection(tokens)
                .filterNot { it.isLink() }
        }, DataTypes.createArrayType(DataTypes.StringType)
    )

    private fun String.isLink() = matches(Regex("^http.*"))
}