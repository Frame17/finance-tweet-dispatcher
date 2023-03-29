package train

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.jetbrains.kotlinx.spark.api.withSpark
import train.load.DatasetLoader
import train.preprocess.TweetPreprocessor


fun main() = withSpark {
    val datasetLoader = DatasetLoader(spark)
    val tweetPreprocessor = TweetPreprocessor(spark).load("tweet-preprocessor-v2")
    val tweetsTrain = datasetLoader.load("train_data.csv")
        .run(tweetPreprocessor::transform)

    val naiveBayes = NaiveBayes()
        .setSmoothing(1.0)
        .setModelType("multinomial")
        .setLabelCol("class")
        .fit(tweetsTrain)

    val evaluator = MulticlassClassificationEvaluator()
        .setLabelCol("class")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")

    val predictionsBayes = datasetLoader.load("valid_data.csv")
        .run(tweetPreprocessor::transform)
        .run(naiveBayes::transform)
    println("Prediction accuracy = " + evaluator.evaluate(predictionsBayes))
    naiveBayes.save("tweet-classifier-v2")
}