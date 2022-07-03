# DbPop - Unit test your database

This utility allows you to quickly re-populate a development database to an initial state
which opens the doors of fast and reliable functional tests.

DbPop currently only supports SQL Server but support for other databases will be added.

DbPop can be invoked from the command line or be used as a dependency to load datasets.
A dataset is a directory containing CSV files corresponding to your database tables.<br/>
For example:

```
/src/
    /test/
        /resources/
            /testdata/
                /base/                          - populates the base test data
                    /AdventureWorks/
                        /HumanResources/
                            /Department.csv     - data files to populate AdventureWorks.HumanResources.Department
                            /Employee.csv       -                        AdventureWorks.HumanResources.Employee
                            /Shift.csv          -                        AdventureWorks.HumanResources.Shift
                /ADV-7412/                      - Additional data specific to bug ADV-7412 
                    /AdventureWorks/
                        /HumanResources/
                            /Employee.csv
```

### Invoking DbPop from your unit test:

You first create a `Populator` instance, you then invoke one of the `load()` methods to load a dataset.
We recommend to create only one `Populator` that you re-use for all unit tests.

To build a `Populator` you need a JDBC connection and a directory that contains your datasets.

```java
public class TestUsage {
    private static Populator populator;

    @BeforeAll
    static void beforeAll() {
        populator = Populator
                .builder()
                .setDirectory("src/test/resources/datasets")
                .setConnection("jdbc:sqlserver://localhost;database=tempdb;trustServerCertificate=true", "sa", "password")
                .build();
    }

    @AfterAll
    static void afterAll() {
        populator.close();
    }

    @Test
    void myTest() {
        populator.load("base");
        // Run some test
    }
}
```

#### JDBC connection

However, the JDBC connection can be built using a property file in your home directory, so you do not have to store a password in your source code:

* Linux: `~/.dbpop.properties`
* Windows: `C:\Users\<username>\dbpop.properties`

```properties
jdbcurl=jdbc:sqlserver://localhost;database=tempdb;trustServerCertificate=true
username=sa
password=yourpassword
verbose=false
```

#### Dataset directory

If you do not specify a dataset directory, DbPop will use `./src/test/resources/datasets`

This simplifies the construction of the `Populator`:

```java
public class TestUsage {
    private static Populator populator;

    @BeforeAll
    static void beforeAll() {
        populator = Populator.build();
    }

    @AfterAll
    static void afterAll() {
        populator.close();
    }

    @Test
    void myTest() {
        populator.load("base");
        // Run some test
    }
}
```

### Invoking DbPop from the command line:

DbPop can be invoked from the command line to download data from the database to CSV files or to upload CSV files into the database.
Both operations require a database connection and a dataset directory.

```text
  -d, --directory=<directory>   Dataset Directory
  -j, --jdbcurl=<dbUrl>         Database URL
  -u, --username=<dbUser>       Database user
  -p, --password=<dbPassword>   Database password
  -v, --verbose                 Verbose
```

You can download individual tables to CSV files using `download tables <table-names>` or schemas using `download tables <schema-names>`<br/>
For example:

```text
download tables \
  --jdbcurl jdbc:sqlserver://localhost \
  --username scott \
  --password tiger \
  sakila.dbo.actor sakila.dbo.address sakila.dbo.category
```



