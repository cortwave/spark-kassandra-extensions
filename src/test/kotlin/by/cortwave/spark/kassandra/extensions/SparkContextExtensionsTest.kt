package by.cortwave.spark.kassandra.extensions

import by.cortwave.spark.kassandra.extensions.model.Add
import by.cortwave.spark.kassandra.extensions.model.User
import org.junit.Test

/**
 * @author Dmitry Pranchuk
 * @since 8/20/16.
 */
class SparkContextExtensionsTest: SparkCassandraTest() {
    @Test
    fun cassandraTableRowsTest() {
        sparkContext.cassandraTableRows(keyspace, "users").foreach { println(it) }
    }

    @Test
    fun cassandraTableTest_User() {
        sparkContext.cassandraTable<User>(keyspace, "users")
        .map { it.email }
        .map { it.split("@")[0] }
        .foreach { println(it) }
    }

    @Test
    fun cassandraTableTest_Add() {
        sparkContext.cassandraTable<Add>(keyspace, "adds")
                .map { it.userEmail }
                .map { it.split("@")[0] }
                .foreach { println(it) }
    }
}