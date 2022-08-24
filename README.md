# simple-de-project
    Building a simple pipeline to extract youtube video data from TwoSetViolin Channel via Youtube API. Then, transforming data using DataProc and loading data to data warehouse which is Google BigQuery.
## Technologies Used
    Cloud Function --> Extract data from Youtube
    Google Cloud Storage --> staging area and data lake
    DataProc --> Transforming data
    BigQuery --> Data Warehouse
    Cloud Composer --> Orchestration tools
## Folder Structure
    In each folder contains code which needs to run with each google cloud products
## Steps
    1. Create buckets to store raw and processed data and upload youtube_category file in raw bucket (console)
    2. Deploy Cloud function to extract data using http as trigger (Web UI)
    3. Create BigQuery Dataset (Web UI)
    4. Create and Run Cloud Composer
## Reference
[TwoSetViolin Channel](https://youtube.com/c/twosetviolin)
[Youtube API Instruction](https://youtu.be/SwSbnmqk3zY)
[Youtube API Documentation](https://developers.google.com/youtube/v3/docs)
[Youtube Video Category ID](https://mixedanalytics.com/youtube-video-category-id-list/)
[Airflow - Dataproc Operator (Pyspark)](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/tests/system/providers/google/cloud/dataproc/example_dataproc_pyspark.html)
[Airflow - GCS to BigQuery Operator](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/gcs.html#howto-operator-gcstobigqueryoperator)
[Airflow - Managing Variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html)
[Airflow - Simple HTTP Operator](https://airflow.apache.org/docs/apache-airflow-providers-http/stable/operators.htm)
[Airflow - Trigger Rule](https://airflow.apache.org/docs/apache-airflow/1.10.5/concepts.html?highlight=trigger%20rule#)