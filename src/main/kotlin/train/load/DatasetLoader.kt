package train.load

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.jetbrains.kotlinx.spark.api.col

class DatasetLoader(
    private val spark: SparkSession
) {
    companion object {
        val datasetSchema: StructType = DataTypes.createStructType(
            arrayOf(
                DataTypes.createStructField("tweet", DataTypes.StringType, false),
                DataTypes.createStructField("class", DataTypes.IntegerType, false)
            )
        )
    }

    fun load(path: String): Dataset<Row> =
        spark.read()
            .schema(datasetSchema)
            .csv(path)
            .filter(col("class").isNotNull)
}