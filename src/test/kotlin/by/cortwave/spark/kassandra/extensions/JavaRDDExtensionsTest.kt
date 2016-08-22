package by.cortwave.spark.kassandra.extensions

import by.cortwave.spark.kassandra.extensions.model.Add
import by.cortwave.spark.kassandra.extensions.model.User
import org.joda.time.Duration
import org.junit.Test

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
                .joinWithCassandraTable<Add, User>(keyspace, "users", mapOf("email" to "userEmail"))
                .map { "${it._1().userEmail} ${it._2.name}" }
                .foreach { println(it) }
    }

    @Test
    fun cassandraJoinWithTest() {
        sparkContext.cassandraTable<Add>(keyspace, "adds")
                .cassandraJoin().with<User>(keyspace, "users", mapOf("email" to "userEmail"))
                .map { "${it._1().userEmail} ${it._2.name}" }
                .foreach { println(it) }
    }
}