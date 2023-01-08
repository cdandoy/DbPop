
# DbPop - The easiest way to populate your development database

This utility allows you to quickly re-populate a development database to an initial state
which opens the doors of fast and reliable functional tests.

DbPop supports SQL Server, PostgreSQL is supported in major releases, support for other databases will be added.

DbPop loads CSV files from your pre-defined datasets.<br/>
All the datasets are under one directory which also contains a configuration file.<br/>
Each dataset is structured like your databases: `catalog`/`schema`/`table.csv`

For example:

```
+-- testdata
   +-- static                               - The static dataset is only loaded once per session
   |    +-- AdventureWorks
   |         +-- HumanResources
   |              |-- CreditCardTypes.csv
   |              +-- Offices.csv
   +-- base                                 - The base dataset is always reloaded
   |    +-- AdventureWorks
   |         +-- HumanResources
   |              |-- Department.csv                           
   |              |-- Employee.csv                     
   |              +-- Shift.csv              
   +-- ADV-7412                             - test data specific to ticket ADV-7412
   |    +-- AdventureWorks
   |         +-- HumanResources
   |              +-- Employee.csv
   |-- dbpop.properties
   +-- setup.sql
```

There are two special datasets:
* **static**: This dataset is only loaded once per session.<br/>
  It should contain the data that is never updated by your application.<br/>
  This is where you would typically store lookup data. The static dataset cannot be loaded explicitely.
* **base**: This dataset is always reloaded first when you load another dataset.<br/>
  It should contain data that you want to always be available and that you want to build upon.<br/>
  You may want for example to always have one customer, with one order and one invoice and another dataset could complete that data with a
  return to test the processing of returns.<br/>
  The base dataset can be loaded explicitely. Smaller applications may only need a base dataset. 

In addition to the two special datasets, this example shows a dataset named ADV-7412, which would be the test data necessary to 
reproduce a corresponding JIRA ticket.

### setup.sql
The optional setup.sql file can be used to set up your database, for example for creating the tables, indexes, ...</br>

## Docker Compose
The easiest way to set up DbPop is by using Docker Compose.

The test data should be stored on your local host, typically under source control.<br/>
A volume will be used to let the DbPop container access the test data.<br/>
Your database will also run in a docker image.

Example of docker-compose.yml:
```dockerfile
version: "3.9"
services:
  dbpop:
    image: "cdandoy/dbpop"
    ports:
      - "7104:7104"
    volumes:
      - c:/git/myapp/dbpop:/var/opt/dbpop
    environment:
      - TARGET_JDBCURL=jdbc:sqlserver://mssql;database=tempdb;trustServerCertificate=true
      - TARGET_USERNAME=sa
      - TARGET_PASSWORD=tiger
#     - SOURCE_JDBCURL=jdbc:sqlserver://qa-db.example.com;database=tempdb;trustServerCertificate=true
#     - SOURCE_USERNAME=sa
#     - SOURCE_PASSWORD=tiger
    depends_on:
      - mssql
  mssql:
    image: mcr.microsoft.com/mssql/server
    ports:
      - "1433:1433"
    environment:
      - SA_PASSWORD=tiger
      - ACCEPT_EULA="Y"
```

* `volumes`: the directory where the CSV files are stored.
* `TARGET_JDBCURL`, `TARGET_USERNAME`, `TARGET_PASSWORD`: How to connect to your test database.
* `SOURCE_JDBCURL`, `SOURCE_USERNAME`, `SOURCE_PASSWORD`: If you have a copy of your production database, DbPop can help 
you create the CSV files. In this example, we have commented out the SOURCE variables but when a source database is available, 
the interface shows additional buttons that allow adding data to the datasets.<br/>
Please, do not connect DbPop to your production database.

Once DbPop is started, open this link: http://localhost:7104 .<br/>
You should now see the datasets you have created, and you can load them by clicking the button on the left of the name.

## REST API
While it is easier for a developer to use the user interface to reload the data during development, you may want to
automate the test using the REST API.

Resetting a dataset is as simple as hitting the URL: http://localhost:7104/populate?dataset=base

You can specify multiple datasets on the same request: http://localhost:7104/populate?dataset=base&dataset=ADV-7412

You can access the API documentation here: http://localhost:7104/api-docs/