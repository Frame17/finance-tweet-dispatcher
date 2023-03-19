package load

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

val datasetSchema: StructType = DataTypes.createStructType(
    arrayOf(
        DataTypes.createStructField("tweet", DataTypes.StringType, false),
        DataTypes.createStructField("class", DataTypes.IntegerType, false)
    )
)

class DatasetLoader(
    private val spark: SparkSession
) {

    fun load(): Pair<Dataset<Row>, Dataset<Row>> {
        val tweetsTrain: Dataset<Row> = spark.read()
            .schema(datasetSchema)
            .csv("train_data.csv")

        val tweetsVal: Dataset<Row> = spark.read()
            .schema(datasetSchema)
            .csv("valid_data.csv")

        return tweetsTrain to tweetsVal
    }
}