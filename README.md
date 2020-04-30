# CreateDBFromShare
In Snowflake, data sharing is region specific, i.e. you can only share data between accounts in the same region. If you want to share data across regions, you first have to create a local copy of that database, and then replicate that local copy into the target region.

CreateDBFromShare is a Snowflake stored procedure that copies tables from a source database & schema to a target database and schema. Source database & schema and target database and schema are provided via command line parameters.

The target schema will be created in case it doesn't exist. If a table in the source doesn't exist, it first will be created.

One main objective was to ensure that changes to existing tables will not result cause queries to fail that run against a shared database of the replicated copy. Therefore, all changes to existing tables must be atomic.

The following modifications to existing tables are supported

1. add a column
1. remove a column
1. rename a column (implemented via adding and removing a column)

Changing the datatype of a column is not supported

The procedure tracks its progress in an array of strings. That "log" along with a status, date and time of execution, and session id is being stored in the log table with every run.

# Setup

Setup of a test environment requires a couple different Snowflake accounts.

1. Originator: This is the Snowflake account that originally shares a dataset via [Snowflake Secure Data Sharing](https://docs.snowflake.com/en/user-guide/data-sharing-intro.html). 
1. Consumer/Replication Source: This is the Snowflake account that consumes the dataset from the Provider. Secondly, this account creates a local copy and replicates the dataset to the replication target. Please review Snowflake's documentation for setting up [replication](https://docs.snowflake.com/en/user-guide/database-replication-failover.html)
1. Replication Target/Provider: This is the Snowflake account that receives the  replicated copy from the replication source. It also is the account that shares the dataset, for instance into a VPS environment

## Setup on Consumer/Replication Source
1. Create a new database that will host the local copy for the shared dataset. To do so, open file setup/create_db.sql (or create the database directly in a Snowflake worksheet), update the database name and run all commands.
1. Create all metadata objects via the files in the metadata directory. The files need to be executed in sequence indicated by the file prefix.

## Setup on Replication Target/Provider


