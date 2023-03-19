import load.DatasetLoader
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.jetbrains.kotlinx.spark.api.withSpark
import preprocess.TweetPreprocessor


fun main() {
    withSpark {
        val tweetPreprocessor = TweetPreprocessor()
        val datasetLoader = DatasetLoader(spark)
        val (tweetsTrain, tweetsVal) = datasetLoader.load()
            .let { (train, valid) -> train.run(tweetPreprocessor) to valid.run(tweetPreprocessor) }

        val naiveBayes = NaiveBayes()
            .setSmoothing(1.0)
            .setModelType("multinomial")
            .setLabelCol("class")
            .fit(tweetsTrain)

        val evaluator = MulticlassClassificationEvaluator()
            .setLabelCol("class")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        val predictionsBayes = naiveBayes.transform(tweetsVal)
        println("Prediction accuracy = " + evaluator.evaluate(predictionsBayes))
    }
}