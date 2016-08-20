package by.cortwave.spark.kassandra.extensions

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.junit.Test

/**
 * @author Dmitry Pranchuk
 * @since 8/18/16.
 */
class SimpleConnectionTest: CassandraTest() {
    @Test
    fun testConnection() {
        session.execute(QueryBuilder.select().all().from("users")).all().forEach { println(it) }
    }
}
