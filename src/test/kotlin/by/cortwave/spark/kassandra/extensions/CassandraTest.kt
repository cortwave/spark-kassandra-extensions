package by.cortwave.spark.kassandra.extensions

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import org.junit.Before

/**
 * @author Dmitry Pranchuk
 * @since 8/20/16.
 */
abstract class CassandraTest {
    protected lateinit var session: Session
    protected val keyspace = "test"

    @Before
    fun setUp() {
        session = Cluster.builder().addContactPoint("127.0.0.1").build().connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        session.execute("USE $keyspace")
        fillWithData(session)
    }
}

