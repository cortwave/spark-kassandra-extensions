package by.cortwave.spark.kassandra.extensions

import scala.Tuple1
import scala.Tuple2
import scala.Tuple3
import scala.Tuple4
import scala.Tuple5
import scala.Tuple6

/**
 * @author Dmitry Pranchuk
 * @since 9/2/16.
 */
object Tuple {
    operator fun<T> invoke(t: T): Tuple1<T> = Tuple1(t)
    operator fun<T1, T2> invoke(t1: T1, t2: T2): Tuple2<T1, T2> = Tuple2(t1, t2)
    operator fun<T1, T2, T3> invoke(t1: T1, t2: T2, t3: T3): Tuple3<T1, T2, T3> = Tuple3(t1, t2, t3)
    operator fun<T1, T2, T3, T4> invoke(t1: T1, t2: T2, t3: T3, t4: T4): Tuple4<T1, T2, T3, T4> = Tuple4(t1, t2, t3, t4)
    operator fun<T1, T2, T3, T4, T5> invoke(t1: T1, t2: T2, t3: T3, t4: T4, t5: T5): Tuple5<T1, T2, T3, T4, T5> = Tuple5(t1, t2, t3, t4, t5)
    operator fun<T1, T2, T3, T4, T5, T6> invoke(t1: T1, t2: T2, t3: T3, t4: T4, t5: T5, t6: T6): Tuple6<T1, T2, T3, T4, T5, T6> = Tuple6(t1, t2, t3, t4, t5, t6)
}