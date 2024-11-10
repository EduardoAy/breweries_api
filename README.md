Case Study - Breweries API

Objective: Automatically obtain data from an external system via API, and create a Data Lake with the medallion architecture according to the data processing level.

1. API: Used the Open Brewery DB API to fetch data. The API has an endpoint for listing breweries:
 <https://api.openbrewerydb.org/breweries>
2. Orchestration tool: Can be chosen according to preference (Airflow, Luigi, Mage etc.) to build a data pipeline. It is important to consider scheduling, retries and error handling in the pipeline.
3. Data Lake Architecture: The data lake should follow the medallion architecture with a bronze, silver, and gold tier:
 a. Bronze tier: Raw API data persisted in its native format or any suitable format.
 b. Silver tier: Data transformed into a columnar storage format, such as parquet or delta, and partitioned by brewery location.
 c. Gold tier: Aggregated view with the number of breweries by type and location.
4. Monitoring/Alert: Considered data quality issues and pipeline failures.

As described above, the following components were used for the solution:
 Cloud Provider: AWS
 Programming language: Python
 External components: Postman API

![image](https://github.com/user-attachments/assets/1c8e1531-6d3f-495a-872f-61621907b03f)


Process Steps
1) An EventBridge Scheduler is configured to trigger executions every 10min;
2) Connected to EventBridge is a Step Functions state machine, for workflow orchestration;
3) A Lambda function connects to EventBridge / Step Functions, to trigger an API query, produce the data as specified and load it into the DataLake, following the “Medallion Architecture” pattern: Bronze, Silver and Gold Layers;
4) Step Functions and Lamba executions are being monitored by the Cloudwatch tool (execution monitoring / Lambda logs);
5) If there is a need to trigger a specific execution (without waiting for automatic scheduling), a REST API was created in Postman for this purpose.
