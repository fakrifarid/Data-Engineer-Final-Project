# Airflow Docker

## Description

This project runs on Docker Compose. To deploy Airflow on Docker Compose, you should fetch [docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml). You could fetch by using this command:

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.4/docker-compose.yaml'
```

This file contains several service definitions:
* `airflow-scheduler` which monitor all tasks and DAGs, then triggers the task instances once their dependencies are completed
* `airflow-webserver` that you may access through `http://localhost:8080`
* `airflow-worker` which executes the tasks given by the scheduler
* `airflow-init` that initialize the service
* `flower` application for monitoring the environment. You may access it at `http://localhost:5555`
* `postgres` as the database
* `redis` roles as broker that forwards messages from scheduler to worker

All these services allow you to run Airflow with [CeleryExecutor](https://airflow.apache.org/docs/apache-airflow/stable/executor/celery.html).

Some directories in the container are mounted, which means that their contents are synchronized between your computer and the container.
* `./dags`, place to put your DAG files
* `./logs`, place which contains logs from task execution and scheduler
* `./plugins`, place to put your [custom plugins](https://airflow.apache.org/docs/apache-airflow/stable/plugins.html) here.

## Settings

After you've downloaded `docker-compose.yaml` file, then there are some configurations required.

### **Initializing Environment**

Before starting Airflow for the first time, we need to prepare the environment, such as creating the necessary files, directories, and initializing the database.

```bash
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### **Initializing Database**

On all operating systems, we need to run database migrations and create the first user account. To do it, run the following command:

```bash
docker-compose up airflow-init
```

After initialization is complete, a message is shown like below.

```bash
airflow-init_1       | Upgrades done
airflow-init_1       | Admin user airflow created
airflow-init_1       | 2.2.4
start_airflow-init_1 exited with code 0
```

The account created has the login `airflow` and the password `airflow`.

### **Running Airflow**

Now, we can start all services by run this command:

```bash
docker-compose up
```

After all services run, you could check on the browser: `http://localhost:8080`. Then, enter username and password from created account above.

![Airflow](https://github.com/ekoteguh10/airflow-docker/raw/main/airflow-login.png)

## Additionals

We also create folders `input` to put datasets for this project and `spark` to put python files running the PySpark.






