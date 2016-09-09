package by.cortwave.spark.kassandra.extensions

import org.junit.BeforeClass
import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.output.WaitingConsumer
import org.testcontainers.containers.wait.Wait

/**
 * @author Dmitry Pranchuk
 * @since 9/10/16.
 */
@RunWith(Suite::class)
@Suite.SuiteClasses(
        JavaRDDExtensionsTest::class,
        RDDExtensionsTest::class,
        SimpleConnectionTest::class,
        SparkContextExtensionsTest::class
        )
class IntegrationTests {
    companion object {
        @BeforeClass
        @JvmStatic
        fun init() {
            val container = CassandraContainer("3.7")
            val waitingConsumer = WaitingConsumer()
            container.start()
            container.followOutput(waitingConsumer, OutputFrame.OutputType.STDOUT)
            waitingConsumer.waitUntil { it.utf8String.contains("Starting listening for CQL clients") }
        }
    }
}

class CassandraContainer(version: String): GenericContainer<CassandraContainer>("cassandra:$version") {
    override fun start() {
        addFixedExposedPort(9042, 9042)
        super.start()
    }
}