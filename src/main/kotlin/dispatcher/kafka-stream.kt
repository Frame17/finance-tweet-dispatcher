package dispatcher

import org.apache.spark.ml.classification.NaiveBayesModel
import org.jetbrains.kotlinx.spark.api.withSpark
import train.preprocess.TweetPreprocessor


fun main() = withSpark {
    val tweetSubscriber = TweetSubscriber(spark)
    val tweetPreprocessor = TweetPreprocessor(spark).load("tweet-preprocessor-v2")
    val tweetClassifier = NaiveBayesModel.load("tweet-classifier-v2")
    val tweetDispatcher = TweetDispatcher(tweetPreprocessor, tweetClassifier)

    tweetSubscriber.readTweets()
        .run(tweetDispatcher::dispatchToTopics)
}