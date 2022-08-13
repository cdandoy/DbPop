# DbPop - Unit test your database

This utility allows you to quickly re-populate a development database to an initial state
which opens the doors of fast and reliable functional tests.

DbPop supports SQL Server and PostgreSQL, support for other databases will be added.

DbPop can be invoked from the command line or be used as a dependency to load datasets.
A dataset is a directory containing CSV files corresponding to your database tables.<br/>
For example:

```
src
+-- test
     +-- resources 
          +-- testdata
               +-- base                                 - populates the base test data
               |    +-- AdventureWorks
               |         +-- HumanResources
               |              |-- Department.csv        - data files to populate AdventureWorks.HumanResources.Department                                  
               |              |-- Employee.csv          -                        AdventureWorks.HumanResources.Employee                              
               |              +-- Shift.csv             -                        AdventureWorks.HumanResources.Shift                          
               +-- ADV-7412                             - test data specific to ticket ADV-7412           
                    +-- AdventureWorks
                         +-- HumanResources
                              +-- Employee.csv
```

## Add DbPop to your build

Add the library to your dependencies via the [Maven package](https://mvnrepository.com/artifact/io.github.cdandoy/dbpop).

Maven:

```xml

<dependency>
    <groupId>io.github.cdandoy</groupId>
    <artifactId>dbpop</artifactId>
    <version>0.0.2</version>
</dependency>
```

Gradle:

```
implementation 'io.github.cdandoy:dbpop:0.0.2'
```

## Invoke DbPop from your unit test:

```java
public class TestUsage {
    private static Populator populator = Populator.builder()
            .setPath("/testdata/")
            .setConnection("jdbc:sqlserver://localhost", "sa", "password")
            .build();

    @Test
    void testSomething() {
        populator.load("base");
        // Run a test
    }

    @Test
    void testSomethingElse() {
        populator.load("base");
        // Run another test
    }

    // more tests...
}
```

## JDBC connection

The JDBC connection can be built using the API or using a property file in your home directory, so you do not have to store a password in your source code

* Linux: `~/.dbpop/dbpop.properties`
* Windows: `C:\Users\<username>\.dbpop\dbpop.properties`

```properties
# Default environment
jdbcurl=jdbc:sqlserver://localhost
username=sa
password=yourpassword

# pgsql environment
pgsql.jdbcurl=jdbc:postgresql://localhost:5432/yourdatabase
pgsql.username=postgres
pgsql.password=yourpassword
```

You use `setEnvironment("pgsql")` on the builder to point to the `pgsql` environment.

## Dataset directory

If you do not specify a dataset directory, DbPop will use `./src/test/resources/datasets`

Using dbpop.properties and the default path simplifies the construction of the `Populator`:

```java
public class TestUsage {
    private static Populator populator = Populator.build();

    @Test
    void myTest() {
        populator.load("base");
        // Run a test
    }
}
```

## Invoke DbPop from the command line:

DbPop can be invoked from the command line using the `download` command to download data from the database to CSV files or the `populate` command to upload CSV files into the database.<br/>

Both operations require a database connection.<br/>
`download` writes the output at the location specified by the `--directory` option.<br/>
`populate` reads the input from the classpath relative to the `--path` option.

```text
Usage: DbPop [-hV] [COMMAND] [options]
Commands:
  help      Displays help information about the specified command
  populate  Populates the database with the content of the CSV files in the specified datasets
  download  Download data to CSV files
Common options:
  --path=<path>                 Dataset path, used in combination with populate
                                Default: /testdata/
  --directory=<directory>       Dataset directory, used in combination with download.
                                Default: ./src/test/resources/testdata/
  --environment=<environment>   dbpop.properties environment
  -j, --jdbcurl=<dbUrl>         Database URL
  -u, --username=<dbUser>       Database user
  -p, --password=<dbPassword>   Database password
```

You can download individual tables, schemas, or whole databases.

Examples:<br/>
Download the content of the 3 tables (`actor`, `address`, `category`) to the corresponding CSV files in src/test/resources/testdata/.

```text
DbPop download\
  --jdbcurl jdbc:sqlserver://localhost \
  --username scott \
  --password tiger \
  sakila.dbo.actor sakila.dbo.address sakila.dbo.category
```

Download the content of the `sakila` database.

```text
DbPop download\
  --jdbcurl jdbc:sqlserver://localhost \
  --username scott \
  --password tiger \
  sakila
```

Upload the content of the `base` directory.

```text
DbPop populate base
```



