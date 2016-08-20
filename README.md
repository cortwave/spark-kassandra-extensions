# spark-kassandra-extensions
Kotlin wrapper for Java Spark-Cassandra API

##Examples

*Briefing*

Table with name **users** exists in **test** keyspace. Table has next structure:

| Column name   | Type            |
| ------------- |:---------------:|
| email         | text PK         |
| age           | int             |
| name          | text            |
| city          | text            |

**User** Java POJO:
```java
public class User {
    private String email;
    private String name;
    private Integer age;
    private String city;
    
    ... //getters and setters
}
```

or **User** Kotlin data class:
```kotlin
data class Users(val email: String, val age: Int, val city: String, val name: String)
```

###Read Cassandra table

* Read cassandra table to untyped RDD (RDD type is **CassandraRow**)

####Java example
```java
CassandraTableScanJavaRDD<CassandraRow> usersUntypedTable = CassandraJavaUtil.javaFunctions(sparkContext)
                                  .cassandraTable("test", "users")
```

####Kotlin example
```kotlin
val usersUntypedTable = sparkContext.cassandraTableRows("test", "users")
```

* Read cassandra table to typed RDD

####Java example
```java
CassandraTableScanJavaRDD<User> usersTable = CassandraJavaUtil.javaFunctions(sparkContext)
                                  .cassandraTable("test", "users", CassandraJavaUtil.mapRowTo(User.class))
```

####Kotlin example
```kotlin
val usersTable = sparkContext.cassandraTable<User>("test", "users")
```

###Read Cassandra table

* save RDD to Cassandra table


