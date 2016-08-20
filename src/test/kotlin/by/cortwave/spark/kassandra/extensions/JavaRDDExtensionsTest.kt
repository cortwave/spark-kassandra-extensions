package by.cortwave.spark.kassandra.extensions

import akka.japi.JAPI
import by.cortwave.spark.kassandra.extensions.model.Add
import by.cortwave.spark.kassandra.extensions.model.User
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.`ColumnName$`
import com.datastax.spark.connector.`SomeColumns$`
import com.datastax.spark.connector.japi.CassandraJavaUtil
import org.joda.time.Duration
import org.junit.Test
import scala.Option

/**
 * @author Dmitry Pranchuk
 * @since 8/20/16.
 */
class JavaRDDExtensionsTest : SparkCassandraTest() {
    @Test
    fun saveToCassandraTest() {
        sparkContext.cassandraTable<User>(keyspace, "users")
                .map { it.copy(name = "<<${it.name}>>") }
                .saveToCassandra(keyspace, "users")
        sparkContext.cassandraTable<User>(keyspace, "users")
                .map { it.name }
                .foreach { println(it) }
    }

    @Test
    fun writeBuilderTest() {
        sparkContext.cassandraTable<User>(keyspace, "users")
                .map { it.copy(age = it.age + 1) }
                .writeBuilder(keyspace, "users")
                .withConstantTTL(Duration.standardSeconds(3))
                .saveToCassandra()
        sparkContext.cassandraTable<User>(keyspace, "users").foreach { println(it) }
        Thread.sleep(4000)
        sparkContext.cassandraTable<User>(keyspace, "users").foreach { println(it) }
    }

    @Test
    fun joinWithCassandraTableTest() {
        sparkContext.cassandraTable<Add>(keyspace, "adds")
                .joinWithCassandraTable<Add, User>(keyspace, "users", someColumns(mapOf("email" to "userEmail")))
                .map { "${it._1().userEmail} ${it._2.name}" }
                .foreach { println(it) }
    }

    fun someColumns(columnNames: Map<String, String>): ColumnSelector {
        val columnsSelection = columnNames.map { `ColumnName$`.`MODULE$`.apply(it.key, Option.apply(it.value)) }
        return `SomeColumns$`.`MODULE$`.apply(JAPI.seq<ColumnRef>(*columnsSelection.toTypedArray()))
    }
}