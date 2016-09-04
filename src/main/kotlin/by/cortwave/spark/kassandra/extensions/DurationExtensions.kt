package by.cortwave.spark.kassandra.extensions

import org.joda.time.Duration

/**
 * @author Dmitry Pranchuk
 * @since 9/4/16.
 */
val Int.millis: Duration
    get() = Duration.millis(this.toLong())

val Int.seconds: Duration
    get() = Duration.standardSeconds(this.toLong())

val Int.minutes: Duration
    get() = Duration.standardMinutes(this.toLong())

val Int.hours: Duration
    get() = Duration.standardHours(this.toLong())

val Int.days: Duration
    get() = Duration.standardDays(this.toLong())

