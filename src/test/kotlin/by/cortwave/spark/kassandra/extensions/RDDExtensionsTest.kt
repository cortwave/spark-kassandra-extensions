package by.cortwave.spark.kassandra.extensions

import org.junit.Assert
import org.junit.Test

/**
 * @author Dmitry Pranchuk
 * @since 8/31/16.
 */
class RDDExtensionsTest : SparkCassandraTest() {
    @Test
    fun sortByValueTest() {
        val testWords = listOf("some", "testing", "words", "to", "the", "kotlin", "a")
        val sorted = sparkContext.parallelize(testWords)
                .mapToPair { Tuple(it, it.length) }
                .sortByValue()
                .map { it._1 }
                .collect()
        Assert.assertArrayEquals(sorted.toTypedArray(), arrayOf("a", "to", "the", "some", "words", "kotlin", "testing"))
    }
}