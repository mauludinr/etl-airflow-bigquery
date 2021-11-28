# ETL SQL to BigQuery with Airflow

## Tools
1) Airflow v2.2.1
2) Python v3.8.10
3) Other Packages
   
## Data
1) Dataset : https://relational.fit.cvut.cz/dataset/Financial. 
   Use the following credentials to access the database:
    - hostname: relational.fit.cvut.cz
    - port: 3306
    - username: guest
    - password: relational
    - database: financial
    - DB Type: MySQL

## Setup
1) Airflow Variables 
   -  Go access your airflow UI (http://localhost:8080), then go to Variables in Admin menu
      
      ![image](https://user-images.githubusercontent.com/38213112/143734679-a2e457ef-b5f3-402d-9151-fc37eca9f972.png)

   -  Add a new record(variable) by clicking the plus (+) icon. Another way is just import variables by creating a json file that contain key value of variables. Like this:
      ```
      {
      "PROJECT_ID": "",
      "BUCKET_NAME": "",
      "GCS_TEMP_LOCATION": "",
      "GCS_STG_LOCATION": "",
      "DATASET_ID": ""
      }
      ```
       -  PROJECT_ID : your Google Cloud Platfrom project id
       -  BUCKET_NAME : your GCS bucket name
       -  GCS_TEMP_LOCATION: your temp location (gs://{yourbucket}/temp)
       -  GCS_STG_LOCATION: your staging data location (gs://{yourbucket}/stag)
   -  You can access your variable from your DAG. Example :
   ```python
   from airflow.models.variable import Variable

   PROJECT_ID = Variable.get("PROJECT_ID")
   ```
   -  Example :
      
      ![image](https://user-images.githubusercontent.com/38213112/143734393-1c014f0e-0887-4004-82bb-74d205336ffc.png)

2) Setup the Airflow Connection 
   -  Run this command : 
      ```
      pip install apache-airflow-providers-google
      ```
      (*https://airflow.apache.org/docs/apache-airflow-providers-google/stable/index.html)
   -  Go access your airflow UI (http://localhost:8080), then go to Connections in Admin menu
   
      ![image](https://user-images.githubusercontent.com/38213112/143735271-d6e9ee38-c5ac-488a-94e3-7eeffefb18aa.png)

   -  Add or Edit current Connection. Search for Google Cloud conn type, then fill some required fields:
      -  Conn Id (set to: google_cloud_default)
      -  Conn Type: google Cloud
      -  Description
      -  Keyfile Path. (locate this path with your keyfile full path)
      -  Keyfile JSON. (if you use the keyfile path leave this blank, otherwise fill this with your google service account key and leave the keyfile path blank)
      -  Number of Retries
      -  Project Id
      -  Click Save button
      
      ![image](https://user-images.githubusercontent.com/38213112/143736053-4cae6351-272e-4fd2-a9de-466c50fcd57c.png)

3) Install other packages 
    ```
    pip install apache-beam[gcp]
    pip install beam-sql-connector==1.8.5
    pip install apache-airflow-providers-apache-beam
    ```
## Execute
1) Run airflow webserver --port 8080 -D in your terminal 
2) Run airflow scheduler in your terminal 
3) Go access your airflow UI (http://127.0.0.1/8080)

   ![image](https://user-images.githubusercontent.com/38213112/143780470-2395508f-5870-4dab-bb38-4617f57e0c54.png)
   
   ![image](https://user-images.githubusercontent.com/38213112/143780555-d228a661-5b95-419b-a6d6-58a94f3248a8.png)

   ![image](https://user-images.githubusercontent.com/38213112/143780610-b2c9b9eb-1296-43d2-a56f-9d0ffe317a9f.png)

