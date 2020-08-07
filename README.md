# Sparkify <img src='https://s3.amazonaws.com/video.udacity-data.com/topher/2018/May/5b06cfa8_3-4-p-query-a-digital-music-store-database1/3-4-p-query-a-digital-music-store-database1.jpg' align="right" height="140" />

This is the third project of Udacitys Data Engineering Nanodegree. In this project, a [AWS Redshift](https://aws.amazon.com/redshift/) database for storing music and artist records is created.

Here are other **Sparkify** projects built on different database:

[Data Modeling: PostgreSQL](https://github.com/kevinkevin556/Sparkify-Postgres)

## Overview

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. As their data engineer, you are tasked with building an ETL pipeline that extracts their data from [S3](https://aws.amazon.com/s3/), stages them in [Redshift](https://aws.amazon.com/redshift/), and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

In this project,

- Data warehousing is implemented with Redshift and S3.
- build an ETL pipeline that first stages the data from JSON files and creates dimension and fact tables based on the staging tables.

### Dimension Tables

- users: users in the app.
- songs: songs in the music database.
- artists: artists in the music database.
- time: timestamps of records in `songplays` broken down into specific units.

### Fact Table

- songplays: records in event data associated with song plays i.e. records with page `NextSong`

### ETL Pipeline

- transfers data from [AWS S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/) into the tables using Redshift
  - Song data: `s3://udacity-dend/song_data`
  - Log data: `s3://udacity-dend/log_data` which should be formatted by following the `s3://udacity-dend/log_json_path.json` json format

## Getting Started

### Prerequisites

These are the Python libraries involved in this project:

```console
psycopg2==2.8.5
```

You can install the depedency by running the following command in the terminal.

```console
foo@bar:~$ pip3 install requirement.txt
```

### Start an Redshift cluster

#### Create an IAM Role for Redshift cluster</bold>

<details>
<summary>
<a class="btnfire small stroke"><em class="fas fa-chevron-circle-down"></em>&nbsp;&nbsp;Show details</a>
</summary>

First, set up an IAM role to give our redshift cluster the access to the AWS S3 bucket.

1. Sign in to the AWS Management Console and open the IAM console at https://console.aws.amazon.com/iam/.
2. In the left navigation pane, choose **Roles**.
3. Choose **Create role**.
4. In the **AWS Service** group, choose **Redshift**.
5. Under **Select your use case**, choose **Redshift - Customizable**, and then **Next: Permissions**.<img src='https://github.com/kevinkevin556/Sparkify-Redshift/blob/master/image/redshift-customizable.png?raw=true' size=50 />
6. On the **Attach permissions policies** page, choose **AmazonS3ReadOnlyAccess**, and then choose **Next: Tags**.<img src='https://github.com/kevinkevin556/Sparkify-Redshift/blob/master/image/s3policy.png?raw=true' size=50 />
7. Skip this page and choose **Next: Review**.
8. For **Role name**, enter any name you want, and then choose **Create Role**.
9. After you have created the IAM role, choose **Role** in the left navigation pane and find it in the list. Double-click the IAM role and you can see the Summary of the role. Find **Role ARN** in the summary and copy the value into `dwh.cfg` file for `ARN` under `IAM_ROLE` section.

</details>

#### Launch an Redshift cluster

<details>
<summary>
<a class="btnfire small stroke"><em class="fas fa-chevron-circle-down"></em>&nbsp;&nbsp;Show details</a>
</summary>

1. Sign in to the AWS Management Console and open the Amazon Redshift console at https://us-west-2.console.aws.amazon.com/redshiftv2.
2. On the Amazon Redshift Dashboard, choose **Create cluster**.
3. For **Cluster identifier**: Enter `redshift-cluster`. And choose `dc2.large`, or you can decide to upgrade your node.<img src='https://github.com/kevinkevin556/Sparkify-Redshift/blob/master/image/cluster-identifier.png?raw=true' s />
4. In the **Database configurations** area, enter the following values **(feel free to use your own setting!)**:
    - **Database name**: Enter `sparkify-db`. Assign this value to the `DB_NAME` in `dwh.cfg` file.
    - **Database port**: Enter `5439`. Assign this value to the `DB_PORT` in `dwh.cfg` file.
    - **Master user name**: Enter `awsuser`. Assign this value to the `DB_USER` in `dwh.cfg` file.
    - **Master user password**: Enter a password for the master user account. Assign this value to the `DB_PASSWORD` in `dwh.cfg` file. <img src='https://github.com/kevinkevin556/Sparkify-Redshift/blob/master/image/database.png?raw=true'  />
5. Unfold **Cluster permissions (optional)**:
    - **Available IAM roles**: Choose the IAM role we have just created in the first step and click `Add IAM role` to apply it.<img src='https://github.com/kevinkevin556/Sparkify-Redshift/blob/master/image/cluster-permission.png?raw=true' size=50 />
6. In the **Additional configurations** area, click `Use defaults` to make modification available.
    - **Network and security**: Select `Yes` for **Publicly accessible**
7. Click **Create cluster**. Wait for 5-10 minute for the cluster to ready.
8. After the Redshift cluster is available, double-click it and copy the link to the endpoint. Remove `:5439/sparkifydb` from the end of the copied value. This is the value of  `HOST` in `dwh.cfg`.<img src='https://github.com/kevinkevin556/Sparkify-Redshift/blob/master/image/endpoint.png?raw=true' size=50 />

Now we complete all the necessary setting for Redshift cluster!

</details>

### Set up the database

Create all tables in our Redshift cluster by running

```console
foo@bar:~$ python create_table.py
```

Then stage the data and create fact table and diemensional tables.

```console
foo@bar:~$ python etl.py
```

**Note: For `create_table.py` and `etl.py`, You can use `-v` or `--verbose` option to print queries as they are executing.**

## Usage

### Python

``` Python
import psycopg2
import configparser

# You can either read the database information from the configuration file:
config = configparser.ConfigParser()
config.read('dwh.cfg')
conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

# Or type it manually:
conn = psycopg2.connect("dbname=sparkifydb \
    host=redshift-cluster.crr5i9wttifi.us-west-2.redshift.amazonaws.com \
    port=5439 \
    user=awsuser \
    password=Passw0rd")  # the argument here should be modified based on your own configuration
cur = conn.cursor()
cur.execute("Some SQL query")
conn.commit()
```

### SQL query in Jupyter Notebook

```Python
%load_ext sql
%sql postgresql://DB_USER:DB_PASSWORD@HOST:DB_PORT/DB_NAME 
#                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#    should be modified based on your own configuration
%sql "...Some SQL queries here..."
```