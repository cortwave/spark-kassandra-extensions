package by.cortwave.spark.kassandra.extensions

import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.japi.CassandraJavaUtil
import com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD
import org.apache.spark.api.java.JavaRDD

/**
 * @author Dmitry Pranchuk
 * @since 8/17/16.
 */
inline fun <reified T : Any> JavaRDD<T>.saveToCassandra(keyspace: String, table: String) {
    CassandraJavaUtil.javaFunctions(this)
            .writerBuilder(keyspace, table, CassandraJavaUtil.mapToRow(T::class.java)).saveToCassandra()
}

inline fun <reified T : Any> JavaRDD<T>.writeBuilder(keyspace: String, table: String): RDDAndDStreamCommonJavaFunctions<T>.WriterBuilder {
    return CassandraJavaUtil.javaFunctions(this).writerBuilder(keyspace, table, CassandraJavaUtil.mapToRow(T::class.java))
}

inline fun <reified T : Any, reified R : Any> JavaRDD<T>.joinWithCassandraTable(keyspace: String,
                                                                                table: String,
                                                                                joinColumns: ColumnSelector): CassandraJavaPairRDD<T, R> {
    return CassandraJavaUtil.javaFunctions(this).joinWithCassandraTable(keyspace,
            table,
            CassandraJavaUtil.allColumns,
            joinColumns,
            CassandraJavaUtil.mapRowTo(R::class.java),
            CassandraJavaUtil.mapToRow(T::class.java))
}

inline fun <reified T : Any> JavaRDD<T>.cassandraJoin(): JoinBuilder<T> {
    return JoinBuilder(this, T::class.java)
}

class JoinBuilder<T>(val rdd: JavaRDD<T>, val tClass: Class<T>) {
    inline fun <reified R : Any> with(keyspace: String,
                                      table: String,
                                      joinColumns: ColumnSelector): CassandraJavaPairRDD<T, R> {
        return CassandraJavaUtil.javaFunctions(rdd).joinWithCassandraTable(keyspace,
                table,
                CassandraJavaUtil.allColumns,
                joinColumns,
                CassandraJavaUtil.mapRowTo(R::class.java),
                CassandraJavaUtil.mapToRow(tClass))
    }
}

