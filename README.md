# **Fraud Detection Analysis**

## Overview:

The objective of this project is to create a data model and ETL flow to gather Fraud Data Information from IEEE-CIS public fraud dataset on Kaggle and provide actionable insights in the form of reports. 

## Scope:

1. The scope of the project is to create a data pipeline which will accept the source files, process them, transform as per the need of the final data model and load them in tables. We are going to use Airflow to orchestrate the job to create tables in Postgres, read the source files and load the data into the data model created in Postgres database.


### Tools:

1. Docker
2. Airflow
3. Postgres
4. Python
5. SQL

## Structure:

```
.
├── Adhoc_Analysis.sql
├── Card_Product_detail_metric.sql
├── Fraud_Amounts_Metric.sql
├── README.md
└── docker-airflow
    ├── Dockerfile
    ├── LICENSE
    ├── README.md
    ├── config
    │   └── airflow.cfg
    ├── dags
    │   ├── __pycache__
    │   │   ├── Fraud_Data_Analysis.cpython-38.pyc
    │   │   ├── fraud_data.cpython-38.pyc
    │   │   ├── main.cpython-38.pyc
    │   │   └── pipeline.cpython-38.pyc
    │   └── pipeline.py
    ├── data
    │   ├── train_identity.csv
    │   └── train_transaction.csv
    ├── docker-compose-LocalExecutor.yml
    ├── plugins
    │   ├── data
    │   │   ├── train_identity.csv
    │   │   └── train_transaction.csv
    │   ├── helpers
    │   │   ├── __init__.py
    │   │   ├── __pycache__
    │   │   │   ├── __init__.cpython-38.pyc
    │   │   │   ├── etl.cpython-38.pyc
    │   │   │   ├── sql.cpython-38.pyc
    │   │   │   └── sql_statements.cpython-38.pyc
    │   │   └── sql.py
    │   └── operators
    │       ├── __init__.py
    │       ├── __pycache__
    │       │   ├── __init__.cpython-38.pyc
    │       │   ├── create_tables.cpython-38.pyc
    │       │   ├── data_analysis.cpython-38.pyc
    │       │   ├── data_quality.cpython-38.pyc
    │       │   └── load_tables.cpython-38.pyc
    │       ├── create_tables.py
    │       ├── data_analysis.py
    │       ├── data_quality.py
    │       └── load_tables.py
    ├── requirements.txt
    └── script
        └── entrypoint.sh

12 directories, 36 files
```

### Data Pipeline Design

The data pipeline was designed using Apache Airflow. The whole process was segregated in several phases:

- Creating the tables in Postgres Database.
- Truncating and Copying the Data from Mount volume to Postgres Database.
- Data Quality checks.
- Providing Analytic Reports. 

Below is the Airflow Dag for the whole process:

![image-20210517153525633](C:\Users\vamsi\AppData\Roaming\Typora\typora-user-images\image-20210517153525633.png)

![image-20210517153648841](C:\Users\vamsi\AppData\Roaming\Typora\typora-user-images\image-20210517153648841.png)

## Development:

#### Airflow:

1. Creating a Postgres Connection.

2. Define a Dag to orchestrate the data pipeline.

6. Error Handling: When using the operator, any failure will propagate to a failure of the airflow task and generate an error message in the logs. 

7. The Pipeline is scheduled to run on a monthly Basis but we can change into monthly or daily depending on the Business needs.

#### Installation and Running Steps:

To run this DAG use the Docker to get an Airflow instance up and running locally:

 1. Install Docker

 2. Clone this repo somewhere locally and navigate to it in your terminal

 3. Navigate to /home/surya/Fraud-Detection-Analysis/docker-airflow Folder.

 4. Please change the ownership by entering command below 

    ​     **sudo chown -R user /home/user/Fraud-Detection-Analysis**

 5. Build the Docker Image by entering the command below:

    ​     **docker build -t puckel/docker-airflow:2.0.0 .**

6. Please Enter the command below to set up the environment:

​            **docker-compose -f docker-compose-LocalExecutor.yml up**

7. Check the Data Source Details:
   1. **/home/user/Fraud-Detection-Analysis/docker-airflow/data/train_identity.csv**
   2. **/home/user/Fraud-Detection-Analysis/docker-airflow/data/train_transaction.csv**

8. Navigate to link below in your browser to see the DAG and setting up necessary configurations to run the DAG. 

​         **- Airflow: [localhost:8080](http://localhost:8080/)**

   1. Make sure you've configured connections: Go to Admin -> Connections and Add a new connection "postgres_conn" and set this values  

      ​         **Conn Id: postgres_conn**

​                **Conn Type : postgres**

​                **Host : postgres**

​                **Port : 5432**

​                **Login : airflow**

​                **Password : airflow**

9. Trigger the DAG and check the steps by clicking on graph view option.  
10. For Adhoc Query Analysis follow the steps below.

​              **1. ** **pgAdmin:** [localhost:5050](http://localhost:5050/)

​              **2. PGADMIN_DEFAULT_EMAIL: pgadmin4@pgadmin.org** 

​              **3. PGADMIN_DEFAULT_PASSWORD: admin**          

​              **4. In the General Tab Please Enter the server Name.** 

​              **5. In the Connection Please Enter the below details:**

​                        **Host : postgres**

​                        **Port : 5432**

​                        **Login : airflow**

​                        **Password : airflow**         

10. Please Enter the command below to tear down the environment:

​            **docker-compose -f docker-compose-LocalExecutor.yml down**



