package by.cortwave.spark.kassandra.extensions

import com.datastax.spark.connector.japi.CassandraJavaUtil
import com.datastax.spark.connector.japi.CassandraRow
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD
import org.apache.spark.api.java.JavaSparkContext

/**
 * @author Dmitry Pranchuk
 * @since 8/17/16.
 */
inline fun <reified T : Any> JavaSparkContext.cassandraTable(keyspace: String, table: String): CassandraTableScanJavaRDD<T> {
    return CassandraJavaUtil.javaFunctions(this).cassandraTable(keyspace, table, CassandraJavaUtil.mapRowTo(T::class.java))
}

fun JavaSparkContext.cassandraTableRows(keyspace: String, table: String): CassandraTableScanJavaRDD<CassandraRow> {
    return CassandraJavaUtil.javaFunctions(this).cassandraTable(keyspace, table)
}