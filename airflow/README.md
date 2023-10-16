# airflow-projects

In this repository, I demonstrate some of my learnings on using the popular tool, Apache Airflow to create, schedule and run data pipelines as well as integration with other big data technologies like BigQuery.

There are two options to run the pipelines:

1. Clone the repo and install dependencies.
2. Follow the step by step setup instructions below to start from scratch.

## Setup

### Requirements

1. Upgrade docker-compose version:2.x.x+
2. Allocate memory to docker between 4gb-8gb
3. Python: version 3.8+

### Set Airflow

1. On Linux, the quick-start needs to know your host user-id and needs to have group id set to 0.
    Otherwise the files created in `dags`, `logs` and `plugins` will be created with root user.

2. Create directory using:

    ```bash
    mkdir -p ./dags ./logs ./plugins
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

3. Download or import the docker setup file from airflow's website

   ```bash
   curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
   ```

4. Create "Dockerfile" use to build airflow container image.

### SetUp GCP for Local System (Local Environment Oauth-authentication)

- Create GCP PROJECT
- Create service account: Add Editor and storage admin, storage object admins and bigquery admin
- Create credential keys and download it
- Change name and location

   ```bash
    cd ~ && mkdir -p ~/.google/credentials/
    mv <path/to/your/service-account-authkeys>.json ~/.google/credentials/google_credentials.json
   ```

- Intall gcloud on system : open new terminal and run    gcloud-sdk : <https://cloud.google.com/sdk/docs/install-sdk>

    ```bash
    gcloud -v
    ```

  to see if its installed successfully
- Set the google applications credentials environment variable

  ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/Users/path/.google/credentials/google_credentials.json"
  ```

- Run gcloud auth application-default login

- Redirect to the website and authenticate local environment with the cloud environment

## Enable API on Google Cloud

1. Enable Identity  and Access management API
2. Enable IAM Service Account Credentials API

## Update docker-compose file and Dockerfile

1. Add google credentials "GOOGLE_APPLICATION_CREDENTIALS" and project_id  and bucket name

    ```none
        GOOGLE_APPLICATION_CREDENTIALS: /.google/credentials/google_credentials.json
        AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: 'google-cloud-platform://?extra__google_cloud_platform__key_path=/.google/credentials/google_credentials.json'

        GCP_PROJECT_ID: "alt-data-engr"
        GCP_GCS_BUCKET: "dte-engr-alt"
    ```

2. Add the below line to the volumes of the airflow documentation

    ```none
    ~/.google/credentials/:/.google/credentials:ro
    ```

3. Create requirements.txt inside the airflow folder and add dependencies as in this repo.
4. Create scripts folder inside the airflow folder and inside it create a entrypoint.sh file (paste dependencies from this repo). Now use it to build airflow container image.
5. Run

  ``` bash
  docker-compose build
  ```

6.Run

  ``` bash
  docker-compose up airflow-init
  ```

7.Run

  ``` bash
  docker-compose up
  ```

8.Inside dags/plugins folder create a web folder
9.Inside web folder, create operators folder and copy over the files from this repo.

## External Resources

Airflow setup: [<https://airflow.apache.org/docs/apache-airflow/stable/start.html>](https://airflow.apache.org/docs/apache-airflow/stable/start.html)

Docker setup instructions: <https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html>

## Conclusion

To restart the project in cases where the docker containers were stopped:

```bash
    docker compose up airflow-init
    docker compose up
```

By this point you should have enough to run the DAG files by simply copying them over to your local setup. Have fun coding...
