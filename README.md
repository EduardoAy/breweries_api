Case Study - Breweries API

1. API: Use the Open Brewery DB API to fetch data. The API has an endpoint for listing breweries:
 <https://api.openbrewerydb.org/breweries>
2. Orchestration Tool: Choose the orchestration tool of your preference (Airflow, Luigi, Mage etc.) to build a data pipeline. We're interested in seeing your ability to handle scheduling, retries, and error handling in the pipeline. 
3. Language: Use the language of your preference for the requests and data transformation. Please include test cases for your code. Python and PySpark are preferred but not mandatory. 
4. Containerization: If you use Docker or Kubernetes for modularization, you'll earn extra points.
5. Data Lake Architecture: Your data lake must follow the medallion architecture having a bronze, silver, and gold layer:
   a. Bronze Layer: Persist the raw data from the API in its native format or any format you find suitable.
   b. Silver Layer: Transform the data to a columnar storage format such as parquet or delta, and partition it by brewery location. Please explain any other transformations you perform.
   c. Gold Layer: Create an aggregated view with the quantity of breweries per type and location.
6. Monitoring/Alerting: Describe how you would implement a monitoring and alerting process for this pipeline. Consider data quality issues, pipeline failures, and other potential problems in your response.
7. Repository: Create a public repository on GitHub with your solution. Document your design choices, trade-offs, and provide clear instructions on how to run your application. 
8. Cloud Services: If your solution requires any cloud services, please provide instructions on how to set them up. Please do not post them in your public repository. 
Evaluation Criteria: Your solution will be evaluated based on the following criteria: 1. Code Quality 2. Solution Design 3. Efficiency 4. Completeness 5. Documentation 6. Error Handling

Conforme a descrição acima, foram utilizados os seguintes componentes para a solução:
Cloud Provider: AWS
Linguagem de programação: Python
Componentes externos: Postman API

![image](https://github.com/user-attachments/assets/784eb2cd-092a-41b9-99ac-ddd648f22ec6)

Etapas do Processo
1) Um EventBridge Scheduler está configurado para disparar execuções a cada 10min;
2) Conectada ao EventBridge, está uma máquina de estado Step Functions, para orquestração do workflow;
3) Uma função Lambda conecta-se ao EventBridge / Step Functions, para disparar um consulta à API, produzir os dados conforme especificado e realizar a carga no DataLake, seguindo o padrão “Medallion Architecture”: Camadas Bronze, Prata e Ouro;
4) As execuções Step Functions e Lamba estão sendo monitoradas pela ferramenta Cloudwatch (monitoramento de execuções / Lambda logs);
5) Caso exista a necessidade de disparar uma execução pontual (sem aguardar o agendamento automátic), foi criada uma API REST no Postman para esta finalidade.
