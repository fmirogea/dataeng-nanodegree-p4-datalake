# dataeng-nanodegree-p4-datalake
This is Project 4: Data Lake, part of Udacity's Nanodegree in Data Engineering.

## Project Description

The current endeavor involves constructing an ETL pipeline which involves extracting data from S3, applying Spark to process it, and then reloading the processed data as a collection of dimensional tables back into S3.

## Architecture Overview

This project's structure is outlined as follows:
- Python is used to write the ETL code, which is then run on an EMR Cluster.
- During development, a Jupyter Notebook connected to the EMR Cluster was utilized. This Notebook is stored in S3 and backed up on GitHub.
- The Data Lake is comprised of log files in CSV format and analytics tables in Parquet format.

![Architecture Overview](/media/Project4_DataLake-Architecture.drawio.png)


## Project files and running the project

After launching an EMR Cluster, you have the option of executing your project through either a Jupyter Notebook or by submitting the Python code

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
The cost of an EMR Cluster is determined by the number of nodes used. In this project, a Cluster consisting of three instances (one master and two core nodes) of type m5.xlarge (with 4vCPU and 16 GB RAM) was utilized for development purposes. The cost for this setup in the USA West (Oregon) region was approximately 0.576 USD per hour at the time of the project's implementation.

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