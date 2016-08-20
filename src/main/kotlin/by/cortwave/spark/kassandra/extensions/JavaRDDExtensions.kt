package by.cortwave.spark.kassandra.extensions

import akka.japi.JAPI
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.`ColumnName$`
import com.datastax.spark.connector.`SomeColumns$`
import com.datastax.spark.connector.japi.CassandraJavaUtil
import com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD
import org.apache.spark.api.java.JavaRDD
import scala.Option

/**
 * @author Dmitry Pranchuk
 * @since 8/17/16.
 */
inline fun <reified T: Any> JavaRDD<T>.saveToCassandra(keyspace: String, table: String) {
    CassandraJavaUtil.javaFunctions(this)
            .writerBuilder(keyspace, table, CassandraJavaUtil.mapToRow(T::class.java)).saveToCassandra()
}

inline fun <reified T: Any> JavaRDD<T>.writeBuilder(keyspace: String, table: String): RDDAndDStreamCommonJavaFunctions<T>.WriterBuilder {
    return CassandraJavaUtil.javaFunctions(this).writerBuilder(keyspace, table, CassandraJavaUtil.mapToRow(T::class.java))
}

inline fun <reified T: Any, reified R: Any> JavaRDD<T>.joinWithCassandraTable(keyspace: String,
                                                                              table: String,
                                                                              joinColumns: Map<String, String>): CassandraJavaPairRDD<T, R> {
    val joinColumnsSelector = someColumns(joinColumns)
    return CassandraJavaUtil.javaFunctions(this).joinWithCassandraTable(keyspace,
            table,
            CassandraJavaUtil.allColumns,
            joinColumnsSelector,
            CassandraJavaUtil.mapRowTo(R::class.java),
            CassandraJavaUtil.mapToRow(T::class.java))
}

fun someColumns(columnNames: Map<String, String>): ColumnSelector {
    val columnsSelection = columnNames.map { `ColumnName$`.`MODULE$`.apply(it.key, Option.apply(it.value)) }
    return `SomeColumns$`.`MODULE$`.apply(JAPI.seq<ColumnRef>(*columnsSelection.toTypedArray()))
}