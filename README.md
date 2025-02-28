# exercise_API_data_ingest

**-The purpose of this project is to practice pulling data from a third party API to have its data ingested into BigQuery.**
<br />
<br />-The third party API site used is https://www.api-ninjas.com/.
<br />-An account needs to be created to utilizing an API key to pull from a HTTP link.  Free accounts limits the type of data information available and usage.
<br />-The free account limits to 10,000 API calls per month (pull requests/records). Some APIs, the result is limited to a default of 1 record.
<br />-For the purpose of this project, an Exercises API was used (https://www.api-ninjas.com/api/exercises).  This API limited the results to 5 records (although in this project, 10 records were possible).

![api_ninja_exercies_api](images/api_ninja_exercises_api.png)

**Details of this project:**
<br />-The focus is do a third party API data ingestion to BigQuery using Google Composer/Apache-Airflow.
<br />-This project is not an ELT or an ETL, but rather, an EL; extract the data and load the data.
<br />-Due to the data out being so limited, three dataset tables were created.
<br />-No transformation and or visualization in Power Bi.

**API Ninja Exercises API**
![www.api-ninjas.com_api_exercises](images/www.api-ninjas.com_api_exercises.png)

**API Data Ingestion Flow Chart**
![api_exercise_workflow](images/api_exercise_workflow.png)

**API Airflow Chart**
![airflow-dag-into-bigquery](images/airflow-dag-into-bigquery.png)

**BigQuery Project View**
![dataset_query_bigquery](images/dataset_query_bigquery.png)



