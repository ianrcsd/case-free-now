# Data Engineering Take Home Test
This take home test focuses on two Airflow DAGs that consume, transform and process data.

## Pre-preparation
### Familiarize yourself with Apache Airflow
In case you have not worked with Airflow in the past it does make sense if you briefly read the introduction of Apache
Airflow. Don't worry though, you do not need to be an Airflow pro to finish the session. A basic understanding of the Airflow
principles is sufficient. You should be know about the following concepts: Dags, Tasks, Operators and Hooks.
This tutorial is a good initial read to understand airflow: https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html

### Setup your local environment
Please set up a working local environment and make sure you can execute one of the example DAGs provided with the Airflow installation.
Have an IDE setup that you are comfortable working with. We recommend running the code in a new virtual environment.
Please make sure to use the standalone installation of airflow to avoid side effects.


## Tasks

### Task 1: Process records from CSV file into SQlite
The first DAG will load the 2 provided datasets `dags/data/booking.csv, dags/data/passenger.csv` into sqlite tables, and process it as following,
1. Store raw csv files into sqlite as tables.
2. Calculate the total number of bookings for new passengers based on their country of origin and save the results into sqlite.
    * New passengers are defined as who registered >= `2021-01-01 00:00:00`.
    * If `country_code` is empty, passengers should be categorized as `OTHER`.
3. Read table data from sqlite based on input dict and print to console.
4. Write unit tests to test your dags and calculations.

`! Note that you must use airflow SqliteOperator or SqliteHook for task 1 implementation instead of sqlite3.`

### Task 2: Streaming records daily from file, aggregate and filter
This time the DAG should stream records from csv files. The files are split by day and you should convert them into `avro` format.  
In the end we would like to filter one value per day.
1. Read file based on date of airflow task instance from list of transactions files `dags/data/transactions_*.csv`.   
   Convert to a binary format and store in temporary file.
2. Read the temporary binary file, sum the values by key and then return the key with the 3rd highest result for the given date.
3. Write unit tests to test your dags and check if your validation works on known cases of wrong data.


### Local testing
To start things of try to run the unit tests.
```bash
make test
```

