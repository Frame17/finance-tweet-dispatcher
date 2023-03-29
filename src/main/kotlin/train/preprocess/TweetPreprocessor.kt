package train.preprocess

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.*
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataTypes
import org.jetbrains.kotlinx.spark.api.SparkSession
import org.jetbrains.kotlinx.spark.api.UDF1
import org.jetbrains.kotlinx.spark.api.col
import scala.collection.JavaConverters
import scala.collection.Seq
import java.io.Serializable


class TweetPreprocessor(
    sparkSession: SparkSession
) : Serializable {

    init {
        sparkSession.udf().register("filterLinksUDF", filterLinksUDF)
    }

    fun fit(dataset: Dataset<Row>): PipelineModel {
        val tokenizer = RegexTokenizer()
            .setInputCol("tweet")
            .setOutputCol("tokens")
            .setPattern("([’']s)?[,!?\"'`:;.—-“”‘’]*\\s+[,!?\"'`:;.—-“”‘’]*")
            .setMinTokenLength(2)

        val stopWords = StopWordsRemover().setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
            .setInputCol("tokens")
            .setOutputCol("tokens_clean")

        val filterLinks = SQLTransformer()
            .setStatement("SELECT *, filterLinksUDF(tokens_clean) AS tokens_filtered FROM __THIS__")

        val hashingTF = HashingTF()
            .setInputCol("tokens_filtered")
            .setOutputCol("tf")

        val idf = IDF()
            .setInputCol("tf")
            .setOutputCol("features")

        val processingPipeline = Pipeline().setStages(arrayOf(tokenizer, stopWords, filterLinks, hashingTF, idf))
        return dataset.filter(col("class").isNotNull)
            .run(processingPipeline::fit)
    }

    fun load(path: String): PipelineModel = PipelineModel.load(path)
}

private val filterLinksUDF = udf(
    UDF1 { tokens: Seq<String> ->
        JavaConverters.asJavaCollection(tokens)
            .filterNot { it.isLink() }
    }, DataTypes.createArrayType(DataTypes.StringType)
)

private fun String.isLink() = matches(Regex("^http.*"))