# Capstone Project
Capstone project for Udacity Data Engineering Nanodegree.

## Summary
The purpose of this project is to find the ratio between total population and total deaths in a year.

In this project,
- PySpark is used to create data frames in Spark.
- Parquet files are created and saved in output directory.
- an ETL pipeline is built using Python which will transform data from data files to dimension and fact tables in Spark using "star" schema.

### Data Sources:
 - [Deaths Data](https://public.opendatasoft.com/explore/dataset/deaths-counts-hmd/table/?dataChart=eyJxdWVyaWVzIjpbeyJjb25maWciOnsiZGF0YXNldCI6ImRlYXRocy1jb3VudHMtaG1kIiwib3B0aW9ucyI6eyJzb3J0IjoiLXllYXIifX0sImNoYXJ0cyI6W3siYWxpZ25Nb250aCI6dHJ1ZSwidHlwZSI6ImxpbmUiLCJmdW5jIjoiQVZHIiwieUF4aXMiOiJ0b3RhbCIsInNjaWVudGlmaWNEaXNwbGF5Ijp0cnVlLCJjb2xvciI6IiNGRjUxNUEifV0sInhBeGlzIjoieWVhciIsIm1heHBvaW50cyI6IiIsInRpbWVzY2FsZSI6InllYXIiLCJzb3J0IjoiIn1dLCJkaXNwbGF5TGVnZW5kIjp0cnVlLCJhbGlnbk1vbnRoIjp0cnVlfQ%3D%3D&refine.country=United+States&sort=year)
 - [Population Data](https://public.opendatasoft.com/explore/dataset/population-hmd/table/?dataChart=eyJxdWVyaWVzIjpbeyJjb25maWciOnsiZGF0YXNldCI6InBvcHVsYXRpb24taG1kIiwib3B0aW9ucyI6eyJzb3J0IjoiLXllYXIifX0sImNoYXJ0cyI6W3siYWxpZ25Nb250aCI6dHJ1ZSwidHlwZSI6ImxpbmUiLCJmdW5jIjoiQVZHIiwieUF4aXMiOiJ0b3RhbCIsInNjaWVudGlmaWNEaXNwbGF5Ijp0cnVlLCJjb2xvciI6IiMxQjY2OTgifV0sInhBeGlzIjoieWVhciIsIm1heHBvaW50cyI6IiIsInRpbWVzY2FsZSI6InllYXIiLCJzb3J0IjoiIn1dLCJkaXNwbGF5TGVnZW5kIjp0cnVlLCJhbGlnbk1vbnRoIjp0cnVlfQ%3D%3D)


### Dimension Tables:
 - dim_country: contains country code and name.
 - dim_deaths: contains deaths per year per country
 - dim_population: contains population per year per country.
 
 ### Fact Table:
 - population_deaths_ratio: contains count of deaths and population per year per country per gender.
 
 
 ### Pre-Req:
 - Python3.6+
 ### How to run:
 - run command to install requirements.
    > pip install requirements.txt
 - run ```python etl.py``` to execute the pipeline to reads data from input dir, processes that data using Spark, and write them output dir.
 
 ## Addressing Other Scenarios:
 #### **The data was increased by 100x.**
    - we can still use the Spark to process the data with increased memory size.
    - we can store data on s3.
    
 #### **The pipelines would be run on a daily basis by 7 am every day.**
    - We can use Airflow to schedule the pipeline at any desired time.
    
 #### **The database needed to be accessed by 100+ people.**
    - we can store the data in Amazon Redshift for fast queries.