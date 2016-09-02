package by.cortwave.spark.kassandra.extensions

import org.junit.Assert
import org.junit.Test

/**
 * @author Dmitry Pranchuk
 * @since 9/2/16.
 */
class TupleTest {
    @Test
    fun tuple1Test() {
        val tuple = Tuple(3)
        Assert.assertEquals(tuple._1, 3)
    }

    @Test
    fun tuple2Test() {
        val tuple = Tuple(3, 5)
        Assert.assertEquals(tuple._1, 3)
        Assert.assertEquals(tuple._2, 5)
    }
}