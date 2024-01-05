## Installation build and running Airflow

Pull the image from the Docker repository.

    docker pull puckel/docker-airflow

Optionally install [Extra Airflow Packages](https://airflow.incubator.apache.org/installation.html#extra-package) and/or python dependencies at build time :

    docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" -t puckel/docker-airflow .
    docker build --rm --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .

or combined

    docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .

Don't forget to update the airflow images in the docker-compose files to puckel/docker-airflow:latest.

By default, docker-airflow runs Airflow with **SequentialExecutor** :

    docker run -d -p 8080:8080 puckel/docker-airflow webserver

If you want to run another executor, use the other docker-compose.yml files provided in this repository.

For **LocalExecutor** :

    docker-compose -f docker-compose-LocalExecutor.yml up -d

NB : If you want to have DAGs example loaded (default=False), you've to set the following environment variable :

`LOAD_EX=n`

    docker run -d -p 8080:8080 -e LOAD_EX=y puckel/docker-airflow

## Running all services

        docker-compose up -d

After all the services are running, you need to do several steps, firstly, create a topic in Kafka, for me this topic is called nasa_apod. Don't forget to set the implementation to 3.

The next step is to create a table in Cassandra, in my case it is done like this.

Enter the container with casandra

        docker exec -it cassandra /bin/bash

Set a login and password

        cqlsh -u cassandra -p cassandra

Сreate a table

        CREATE KEYSPACE nasa WITH replication = {'class':'SimpleStrategy','replication_factor':1};

        CREATE TABLE nasa_apod (
            date text PRIMARY KEY,
            title text,
            explanation text,
            url text,
            hdurl text
        );
Next, copy the spark file to the container

        docker cp spark_streaming.py spark_master:/opt/bitnami/spark/

Now we can start the PySpark application

        docker exec -it spark_master /bin/bash

        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 spark_streaming.py

You can exit the container leaving the process running

## UI Links

- Airflow: [localhost:8080](http://localhost:8080/)
- Kafka: [localhost:8888](http://localhost:8888/)


## Result

We have two dags in Airflow, one starts the process of transferring data to kafka once a day, and uploading this data using SparkStreaming to Cassandra. And the second one, which uses data from the database to send a post in the telegram channel https://t.me/nasa_pictureoftheday
