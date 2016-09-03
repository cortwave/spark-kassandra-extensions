package by.cortwave.spark.kassandra.extensions

import akka.japi.JAPI
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.`ColumnName$`
import com.datastax.spark.connector.`SomeColumns$`
import com.datastax.spark.connector.japi.CassandraJavaUtil
import scala.Option

/**
 * @author Dmitry Pranchuk
 * @since 9/3/16.
 */
object Columns {
    operator fun invoke(vararg columnAlias: Pair<String, String>): ColumnSelector {
        return someColumns(columnAlias.toMap())
    }

    operator fun invoke(vararg columns: String): ColumnSelector {
        return CassandraJavaUtil.someColumns(*columns)
    }

    private fun someColumns(columnNames: Map<String, String>): ColumnSelector {
        val columnsSelection = columnNames.map { `ColumnName$`.`MODULE$`.apply(it.key, Option.apply(it.value)) }
        return `SomeColumns$`.`MODULE$`.apply(JAPI.seq<ColumnRef>(*columnsSelection.toTypedArray()))
    }
}