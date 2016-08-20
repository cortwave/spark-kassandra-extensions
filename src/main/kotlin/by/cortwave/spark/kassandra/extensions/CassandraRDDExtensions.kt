package by.cortwave.spark.kassandra.extensions

import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.util.JavaApiHelper.getClassTag
import scala.Tuple2

/**
 * @author Dmitry Pranchuk
 * @since 8/17/16.
 */
inline fun <reified K : Any, reified V : Any> CassandraRDD<Tuple2<K, V>>.toJavaPairRDD(): CassandraJavaPairRDD<K, V> {
    return CassandraJavaPairRDD<K, V>(this, getClassTag(K::class.java), getClassTag(V::class.java))
}

inline fun <reified T: Any> CassandraRDD<T>.toJavaRDD(): CassandraJavaRDD<T> {
    return CassandraJavaRDD<T>(this, getClassTag(T::class.java))
}