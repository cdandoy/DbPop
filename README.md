# DbPop - Unit test your database

This utility allows you to quickly re-populate a development database to an initial state 
which opens the doors of unit-testing databases.

DbPop can be invoked from the command line or be used as a dependency to load datasets.
A dataset is a directory containing CSV files corresponding to your database tables.

```
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
TBD

### Invoking DbPop from the command line:

```text
Usage: DbPop [-hvV] [-d=<directory>] [-j=<dbUrl>] -p=<dbPassword> [-u=<dbUser>]
             <dataset>...
      <dataset>...          Datasets
  -d, --directory=<directory>
                            Dataset Directory
  -h, --help                Show this help message and exit.
  -j, --jdbcurl=<dbUrl>     Database URL
  -p, --password=<dbPassword>
                            Database password
  -u, --username=<dbUser>   Database user
  -v, --verbose             Verbose
  -V, --version             Print version information and exit.
```