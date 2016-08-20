# spark-kassandra-extensions
Kotlin wrapper for Java Spark-Cassandra API. Extends `JavaSparkContext` and `JavaRDD` functionality. Simplifies working with Cassandra data using Apache Spark with Java API.

##Getting started

###Gradle (Jitpack dependency)
```gradle
repositories {
    ...
    maven { url "https://jitpack.io" }
}

compile 'com.github.cortwave:spark-kassandra-extensions:0.1.0'
```

##Examples

*Briefing*

Table with name **users** exists in **test** keyspace. Table has next structure:

| Column name   | Type            |
| ------------- |:---------------:|
| email         | text PK         |
| age           | int             |
| name          | text            |
| city          | text            |

`User` Java POJO:
```java
public class User {
    private String email;
    private String name;
    private Integer age;
    private String city;
    
    ... //getters and setters
}
```

or `User` Kotlin data class:
```kotlin
data class Users(val email: String, val age: Int, val city: String, val name: String)
```

###Read Cassandra table

* Read cassandra table to untyped RDD (RDD type is `CassandraRow`)

####Java example
```java
CassandraTableScanJavaRDD<CassandraRow> usersUntypedTable = CassandraJavaUtil.javaFunctions(sparkContext)
                                  .cassandraTable("test", "users");
```

####Kotlin example
```kotlin
val usersUntypedTable = sparkContext.cassandraTableRows("test", "users")
```

* Read cassandra table to typed RDD

####Java example
```java
CassandraTableScanJavaRDD<User> usersTable = CassandraJavaUtil.javaFunctions(sparkContext)
                                  .cassandraTable("test", "users", CassandraJavaUtil.mapRowTo(User.class));
```

####Kotlin example
```kotlin
val usersTable = sparkContext.cassandraTable<User>("test", "users")
```

###Save to Cassandra table

*users* type - `JavaRDD<User>`

* save RDD to Cassandra table

####Java example
```java
CassandraJavaUtil.javaFunctions(users)
            .writerBuilder("test", "users", CassandraJavaUtil.mapToRow(User.class)).saveToCassandra();
```

####Kotlin example
```kotlin
users.saveToCassandra("test", "users")
```


