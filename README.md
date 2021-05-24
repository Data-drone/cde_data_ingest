# CDE Example Data Ingestion


This is an example of how to build out a Cloudera Data Engineering Job,

## Structure

PySpark and Airflow code is under: `src/main/python`

This consists of two PySpark jobs:
- `load_data.py` - reads data from aws open data
    - See: https://community.cloudera.com/t5/Community-Articles/External-AWS-Bucket-Access-in-CDP-Public-Cloud/ta-p/302074 for accessing outside buckets in CDP

- `etl_job.py` - transforms data into an final reporting table

- `airflow_job.py` - Airflow DAG for schduling the sequence of tasks

## Airflow Notes

Airflow jobs has to consist of a single DAG

CDE Jobs are defined with `CDEJobRunOperator` the execution order of the jobs can then be set with

```python

start >> job1 >> etl_job >> end

```

## Further Reading

Walmart Global Tech - Airflow Beginners Guide: https://medium.com/walmartglobaltech/airflow-the-beginners-guide-684fda8c87f8

