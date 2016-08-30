package by.cortwave.spark.kassandra.extensions

import org.apache.spark.api.java.JavaPairRDD

/**
 * @author Dmitry Pranchuk
 * @since 8/30/16.
 */
fun <K, V> JavaPairRDD<K, V>.sortByValue(ascending: Boolean = true): JavaPairRDD<K, V> {
    return this.mapToPair { it.swap() }.sortByKey(ascending).mapToPair { it.swap() }
}