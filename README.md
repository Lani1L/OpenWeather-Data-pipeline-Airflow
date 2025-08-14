# Data-pipeline-Airflow

<img width="1051" height="391" alt="image" src="https://github.com/user-attachments/assets/0decf7c1-607d-4cb5-968a-dd2baf81aa37" />


A lightweight ETL pipeline that extracts weather data for all Asian countries using the OpenWeather API and stores the results in AWS S3. The workflow is orchestrated by Apache Airflow running on EC2, ensuring reliable and automated orchestration. The pipeline follows best practices by encapsulating logic within a single function for readability and maintainability, respecting API rate limits, and securely managing sensitive information such as API keys. The resulting cleaned weather dataset is now ready for downstream analytics and data-driven insights.
