# DATA WAREHOUSE - Project 3 
This is a learning project with the aim to apply the learnings on on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, it will be needed to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Table of Contents
* [General Info](#general-information)
* [Technologies Used](#technologies-used)
* [Features](#features)
* [Setup](#setup)
* [Usage](#usage)
* [Project Status](#project-status)
* [Room for Improvement](#room-for-improvement)
* [Acknowledgements](#acknowledgements)
* [Contact](#contact)
<!-- * [License](#license) -->


## General Information
This project is presented in a real case context where Sparkify, a music streaming firm, wants to assess the information they've gathered on songs and user activity on their new app. They have grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The idea is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## Technologies Used
- Python - version 3.0

## Features
List the ready features here:
1. Star schema relationa DB based on 5 tables.

This star schema is based on 4 dimensional tables (songs, artists, time and users) which have a unique identifier which is the primery key that will be linked to the fact table (songplays). This configuration is the most optimal for the raw data that we have, since there isn't a big amount of dimensions and we do not need 1-to-many connections. It allows a certain lever of denormalization wich makes the information more accessible. 

2. A sql_queries script to extract, transform, load the raw data from s3 to Redshift.

3. A Jupyter Notebook to create a Redshift Cluster directly with the code. We create also a IAMrole with reading permissions to extract information from s3 and load it to Redshift. 

## Setup
To run this project locally, I wold reccommend to create an environment with "virtual environment" 
Web to create a venv [_here_](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/).

Then, it will be needed to install these packages to make run the project.
- pip install postgres
- pip install os-sys
- pip install pip install pandas
- pip install boto3
- pip install psycopg2

## Usage

We have applied a ETL pipeline in which there is a connection, using AWS keys, to the public s3 bucket. There, there are all the JSON files. This information is extracted and transformed to a star schema organization. Then, the transformed tbales are uploaded to a data warehouse repository in AWS calles Redshift. 


To run the project it is needed to run this statement in the terminal:

`python -m create_ables
python -m etl`


## Project Status
Project is: _complete_ 


## Room for Improvement

Examples of queries that we could create 

See if there are preferences on artists between girls and boys: 

> SELECT users.user_id, users.gender, artists.artist_id, artists.name FROM ((users JOIN songplays ON users.user_id = songplay.suser_id) JOIN artists ON songplay.artist_id = artists.artist_id)

## Acknowledgements
- Udacity team.
- colleges from the online course


## Contact
Created by [@maullaina] maullaina@gmail.com