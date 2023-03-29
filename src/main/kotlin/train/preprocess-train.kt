package train

import org.jetbrains.kotlinx.spark.api.withSpark
import train.load.DatasetLoader
import train.preprocess.TweetPreprocessor

fun main() = withSpark {
    val datasetLoader = DatasetLoader(spark)
    val dataset = datasetLoader.load("train_data.csv")

    val tweetPreprocessor = TweetPreprocessor(spark)
    val preprocessModel = tweetPreprocessor.fit(dataset)
    preprocessModel.save("tweet-preprocessor-v2")
}