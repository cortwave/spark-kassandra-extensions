package by.cortwave.spark.kassandra.extensions

import by.cortwave.spark.kassandra.extensions.model.Add
import by.cortwave.spark.kassandra.extensions.model.User
import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import java.util.Random
import java.util.UUID

/**
 * @author Dmitry Pranchuk
 * @since 8/18/16.
 */

private val cities = arrayOf("Minsk", "Moscow", "Rome", "Prague")
private val names = arrayOf("John", "Marry", "Doe", "Kevin", "Vlad", "Oleg")
private var id = 1
private val rand = Random()

fun fillWithData(session: Session) {
    initSchema(session)
    insertValues(session)
}

private fun initSchema(session: Session) {
    session.execute("CREATE TABLE IF NOT EXISTS users (email text PRIMARY KEY , age int, city text, name text)")
    session.execute("CREATE TABLE IF NOT EXISTS adds (id int PRIMARY KEY, user_email text, content text, rate double)")
}

private fun insertValues(session: Session) {
    val users = Array(50, { generateUser() })
    val emails = users.map { it.email }.toTypedArray()
    val adds = Array(200, { generateAdd(emails) })
    users.forEach { session.execute(insertQuery(it)) }
    adds.forEach { session.execute(insertQuery(it)) }
}

private fun insertQuery(user: User): Insert {
    return QueryBuilder.insertInto("users")
            .value("email", user.email)
            .value("age", user.age)
            .value("name", user.name)
            .value("city", user.city)
}

private fun generateUser(): User {
    val city = randomElement(cities)
    val name = randomElement(names)
    val age = rand.nextInt(100)
    val email = "$name${id++}@gmail.com"
    return User(email, age, city, name)
}

private fun insertQuery(add: Add): Insert {
    return QueryBuilder.insertInto("adds")
            .value("content", add.content)
            .value("id", add.id)
            .value("rate", add.rate)
            .value("user_email", add.userEmail)
}

private fun generateAdd(generatedEmails: Array<String>): Add {
    val content = UUID.randomUUID().toString()
    val id = id++
    val rate = rand.nextDouble() * 10
    val userEmail = randomElement(generatedEmails)
    return Add(userEmail, id, content, rate)
}

private fun <T> randomElement(array: Array<T>): T {
    val index = rand.nextInt(array.size)
    return array[index]
}