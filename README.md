# Working with Cloudera Data Engineering From 0 to Productivity!

This article goes through all the steps to go becoming productive with Cloudera Data Engineering on CDP-Public Cloud.
Cloudera Data Engineering is Cloudera's new Spark as a Service offering on Public Cloud. It features kubernetes auto-scaling of spark workers for cost optimisations, a simple UI interface for job management and an integrated Airflow Scheduler for developing production grade workflows. 

## Coding for Cloudera Data Engineering

Cloudera Data Engineer (CDE) can take Python and Scala jobs as inputs. Depending on which language you wish to use the steps to develop on CDE will be slightly different.

## Repo Structure

The example PySpark and Airflow code is under: `src/main/python`

This consists of two PySpark jobs:
- `load_data.py` - reads data from aws open data
    - See: https://community.cloudera.com/t5/Community-Articles/External-AWS-Bucket-Access-in-CDP-Public-Cloud/ta-p/302074 for accessing outside buckets in CDP

- `etl_job.py` - transforms data into an final reporting table

- `airflow_job.py` - Airflow DAG for schduling the sequence of tasks

Scala Code is available under: `src/main/scala`

This consists of two Scala jobs:
- `LoadData.scala`
- `ETLJob.scala`

### Building Jobs for CDE - General Concepts

With CDE, the executor settings like `num_executors` and additional spark configuration flags that are normally set as part of the `SparkSession` are set via the CDE UI or CLI and do not need to be set in the code. Including them in the code as well can result in abnormal behaviour.

### Building Jobs for CDE - Python

Exploring `load_data.py`, you can see that the `SparkSession` segment is quite consise. With the Cloudera SDX layer, the setting of kerberos settings and aws iam roles is also handled for the user. Simply initialise the SparkSession in the script then start developing your ETL process.

To deploy the `load_data` script, we can simply define it as a new job in the CDE UI.

For this particular example, we setup the load_data script used the following settings:
![Load Data Settings](images/load_data_config.png)

Note the `Configurations` section, these are the flags and values that we would normally set when initialising the `SparkSession`.

<TODO - Python dependency management?>

Note in particular the bucket settings, this is because we need for spark to be able to access both the AWS open data `s3a://nyc-tlc` bucket and our standard configured cdp datalake bucket which in this case is `s3a://blaw-sandbox2-cdp-bucket`.


### Building Jobs for CDE - Scala

Ensure that you have sbt installed and can build scala projects.
See: https://mungingdata.com/apache-spark/building-jar-sbt/

There are two options for building your jars. fat jars or slim jars.

#### Slim Jars

Slim jars mean that any external dependencies need to be manually uploaded in the UI or CLI. See below:
![Upload Dependencies](images/uploading_dependencies.png)

#### Fat Jars

With complex dependencies, a fat jar that comes prepackaged with all the necessary dependencies can be easier to handle.


## Setting up CDE and Airflow

Setup `load_data.py` and `etl_job.py` as individual jobs in CDE.
![Jobs Screen](images/Jobs_screen.png) including setting up the executor settings and `hadoopFileSystems`.

note down the names of the jobs as you have entered them.

In the `airflow_job.py` the job_names need to be entered as per what was entered into the cli ie

```{python}

# job_name needs to match what is in the CDE UI
load_data_job = CDEJobRunOperator(
    task_id='loader',
    dag=cde_process_dag,
    job_name='load_data_job'
)

```
## Airflow Notes

Airflow jobs has to consist of a single DAG

CDE Jobs are defined with `CDEJobRunOperator` the execution order of the jobs can then be set with

```python

start >> load_data_job >> etl_job >> end

```

Note that branching is supported as well so we could do something like this in a more advanced `DAG`

```python

start >> load_data_job
load_data_job >> etl_job_1
load_data_job >> etl_job_2

```

Both fanning out from `load_data_job` to multiple `etl_job` and fanning back in from `etl_job` to some sort of summary job is supported.

## Further Reading

Walmart Global Tech - Airflow Beginners Guide: https://medium.com/walmartglobaltech/airflow-the-beginners-guide-684fda8c87f8

Understanding Cron timers: https://crontab.guru/#*/30_*_*_*_*

Understanding Airflow DAGs: https://towardsdatascience.com/airflow-how-and-when-to-use-it-2e07108ac9f5