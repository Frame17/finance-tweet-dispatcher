package dispatcher

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.jetbrains.kotlinx.spark.api.SparkSession
import org.jetbrains.kotlinx.spark.api.col


class TweetSubscriber(
    private val spark: SparkSession
) {

    companion object {
        private val schema: StructType = DataTypes.createStructType(
            arrayOf(
                DataTypes.createStructField("tweet", DataTypes.StringType, false)
            )
        )
    }

    fun readTweets(): Dataset<Row> = spark.readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9093")
        .option("subscribe", "financial-tweets")
        .option("startingOffsets", "earliest")
        .load()
        .withColumn("value", functions.from_json(col("value").cast("string"), schema))
        .withColumn("tweet", col("value.tweet"))
        .filter(col("tweet").isNotNull)
}