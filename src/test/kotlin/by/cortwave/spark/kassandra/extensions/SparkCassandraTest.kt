package by.cortwave.spark.kassandra.extensions

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

/**
 * @author Dmitry Pranchuk
 * @since 8/20/16.
 */
abstract class SparkCassandraTest : CassandraTest() {
   val sparkContext = SparkContext.context
}

object SparkContext {
    private val sparkConf = SparkConf().setMaster("local[*]").setAppName("test")
    val context = JavaSparkContext(sparkConf)
}