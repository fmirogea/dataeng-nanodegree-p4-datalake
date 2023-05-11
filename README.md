# dataeng-nanodegree-p4-datalake
This is Project 4: Data Lake, part of Udacity's Nanodegree in Data Engineering.

## Project Description

In this project we are building an ETL pipeline that extracts data from S3, processes it using Spark, and loads the data back into S3 as a set of dimensional tables 

## Architecture Overview

The architecture of this project is defined as follows.
- The ETL code is writen in Python and executed in an EMR Cluster.
- For the Development a Jupyter Notebook connected to the EMR Cluster was used. The Jupyter Notebook itself is stored in S3 and backed-up in GitHub.
- The Data Lake is a set of log Files (in csv format) and analytics tables (in parquet format)

![Architecture Overview](/media/Project4_DataLake-Architecture.drawio.png)


## Project files and running the project

Once started an EMR Cluster, you can run the project either using a Jupyter Notebook or by submitting the python code.

### Using the Jupyter Notebooks
- In the AWS Console look for Notebooks in EMR.
- Create a Notebook and attach it to the running cluster.
- Open the Notebook in Jupyter and follow the steps in the Notebook


### Submitting the Python code

- Connect to your EMR cluster using SSH.
- Navigate to the directory where your etl.py script is located.
- Run the following command to submit the script to the EMR cluster:
    spark-submit etl.py
    This command will execute the etl.py script using Spark on the EMR cluster.

You can monitor the progress of the job using the EMR console or the YARN Resource Manager UI.

Once the job is complete, you can verify the output in the S3 bucket or any other location that you have specified in the script.

## Costs 


## File descriptions
Different files and folders can be found in the repository::

- The `etl.py` script contains the code necessary for running the pipeline.
- The `Sparkify.ipynb` Notebook for interactive development

## Database schema and ETL pipeline design
The data model can be described as follows:

![Data Model](/media/Project4_DataLake-DataModel.drawio.png)

- Five analytical tables have been defined, following a Star Schema, optimizing for providing fast answers to analytical queries.
- The `songs` table is partitioned by year and artist.
- The `time`, `songplays` tables are partitioned by year and month.