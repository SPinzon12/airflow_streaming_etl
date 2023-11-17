# airflow_streaming_etl
The "airflow_streaming_etl" project entails integrating diverse data sources, creating an Airflow DAG for ETL tasks and database storage, implementing Kafka for real-time metric streaming, extracting data for dashboard creation in Power BI , setting up a Kafka consumer in a Python app, and ultimately crafting a real-time dashboard for visualizing the streaming data.

## About the Data

### API - Jikan API

The project relies on the Jikan API to gather anime-related data. This API facilitates seamless access to MyAnimeList.net data, eliminating the need for an API key and simplifying the retrieval of information related to anime.

The goal with this dataset is to conduct a descriptive analysis, aiming to understand the distribution of anime titles, genres, user ratings, and other essential features. Through this analysis, we aim to unveil critical insights into the world of anime and manga.

For more in-depth information about the Jikan API, refer to the [official documentation](https://docs.api.jikan.moe/#section/Information).

### CSV - US Used Cars Dataset

The primary dataset utilized in this project is the "US Used Cars Dataset." This extensive dataset comprises a total of 3,000,040 rows and 66 columns. Each row represents a unique record of a used vehicle, and each column corresponds to a specific feature associated with the vehicles. The dataset serves as a comprehensive source for exploring and analyzing the characteristics of used cars in the United States.

This project aims to leverage the dataset for reliable price forecasting, providing valuable insights for buyers, sellers, and stakeholders in the automotive industry.

You can access the dataset [here](https://www.kaggle.com/datasets/ananaymital/us-used-cars-dataset).


## About the Model

This project utilizes a Random Forest Regressor, a powerful machine learning algorithm, to predict used car prices. The model is implemented using the `RandomForestRegressor` function from the `sklearn` library.

The Random Forest Regressor model achieved an R-squared of 0.82, indicating a strong performance in capturing the variance in used car prices.


## Project Structure
```
├── airflow_dag
│   ├── api_etl.py
│   ├── dag.py
│   ├── db_etl.py
│   ├── kafka_stream.py
│   └── services
│       ├── __init__.py
│       ├── api.py
│       ├── db_postgres.py
│       └── kafka.py
├── app
│   ├── data_extraction.py
│   └── kafka_consumer.py
├── model
│   └── random_forest_regressor.rar
├── notebooks
│   ├── anime_eda.ipynb
│   ├── used_cars_eda_3m.ipynb
│   └── used_cars_eda.ipynb
├── docker-compose.yml
└── requirements.txt
```
The `airflow_dag` directory includes scripts and modules for managing Airflow tasks. The `app` directory contains scripts for Kafka streaming and project initialization. The `model` directory holds the model for predicting used car prices. The `notebooks` directory includes exploratory data analysis (EDA) notebooks. The `docker-compose.yml` file is the Docker Compose configuration, incorporating both Kafka and the database setup. and `requirements.txt` lists the necessary Python libraries.

## Getting Started

To utilize this project, please follow these steps:

1. **Clone Repository:** Clone this repository to your local machine.

2. **Ensure Python Installation:** Ensure that you have Python installed on your system.

3. **Database Setup:** Create a `db_config.json` file in the root directory of the project and configure it with the appropriate database credentials. Use the following JSON structure as a template, replacing placeholders with your actual credentials:

    ```json
    {
        "user": "your_postgres_user",
        "password": "your_postgres_password",
        "database": "your_postgres_database"
    }
    ```

4. **Create a Virtual Environment:**
   Create a virtual environment for this project. You can do this by executing the following command in your terminal:

   ```bash
   python -m venv environment_name
   ```

5. **Activate the Virtual Environment:**
   - Next, activate the virtual environment (commands may vary depending on your operating system):
     - En Windows:

       ```bash
       environment_name\Scripts\activate
       ```

     - En macOS y Linux:

       ```bash
       source environment_name\Scripts\activate/bin/activate
       ```

6. **Install Dependencies:** Begin by installing the necessary dependencies. Run the following command in your terminal to install them:

   ```bash
   pip install -r requirements.txt
   ```

7. **Airflow Configuration:**

   Assuming that Airflow is already installed in your repository folder, you need to configure the `airflow.cfg` file. In the `dags_folder` section, make sure to specify the path to the DAGs. Replace `dags` with `airflow_dag` so it looks like the following:

   ```bash
   dags_folder = /root/airflow_workshop/airflow_dag
   ```
   Additionally, run the following command to set the AIRFLOW_HOME environment variable while in the root of the repository:

    ```bash
    export AIRFLOW_HOME=$(pwd)
    ```

8. **Setting up Kafka:**
   - Run Docker Compose:
        ```bash
        docker-compose up
        ```
   - Access Kafka Container:
        ```bash
        docker exec -it kafka bash 
        ```
   - Create Kafka Topic:
        
        Inside the Kafka container, run the following command to create a Kafka topic named `kafka-used-cars`:
        ```bash
        kafka-topics --bootstrap-server kafka --create --topic kafka-used-cars 
        ```  
        This command sets up a Kafka topic that will be used for streaming used car data.

9. **Setting up Real-Time Dashboard:**
   - Unzip the file `model/random_forest_regressor.rar`.
   - Follow this guide [link](https://desarrollopowerbi.com/dashboard-en-tiempo-real-con-apacha-kafka-python-y-power-bi/) to obtain the `API_ENDPOINT`.

        Ensure that the structure of the data is as follows:
        ```json
        [
            {
                "price": 98.6,
                "price_prediction": 98.6,
                "time": "2023-11-17T14:43:55.005Z"
            }
        ]
        ```
   - Edit the file airflow_dag/services/kafka.py in the following section:
      ```python
      API_ENDPOINT = "GENERATED_POWERBI_API_URL"
      ```

10. **Get and Insert Dataset:**
  Download the dataset from Kaggle and insert it into a `./data` folder.

11. **Run Data Extraction:**
   - Execute the `app/data_extraction.py` script.
   - By default, the script will read all 3,000,000 rows from the dataset, which may take a substantial amount of time.
   - To run the script with a specific number of rows, add the `nrows` parameter when reading the CSV file in the script itself. For example:
     ```python
     # Example: Read the first 1000 rows from the dataset
     cars_df = pd.read_csv("./dataset/used_cars_data.csv", dtype={"dealer_zip": str}, nrows=1000)
     ```

12. **Run the DAG in Airflow:**
  
    Once you have configured Airflow and your DAGs are in the correct location, start Airflow using the following command at the root of the repository:
   
    ```bash
    airflow standalone
    ```
    Next, log in to the Airflow dashboard and look for the DAG with the name `"proyect_dag"` Execute this DAG to initiate the ETL process and process the data.

    Note: Ensure that all the preceding steps have been successfully completed before running the DAG in Airflow.

13. **Running the Stream:**
    - When Airflow is in the Kafka task, in a terminal, execute the file `app/kafka_consumer.py`. If you encounter issues, configure the `PYTHONPATH` at the root of the repository as follows:

      On Windows, set the `PYTHONPATH` environment variable with the following command:

      - On Windows:
      ```cmd
      $env:PYTHONPATH += ";$(Get-Location)"
      ```

      - On Linux and macOS:
      ```bash
      export PYTHONPATH="${PYTHONPATH}:$(pwd)"
      ```

      After configuring the `PYTHONPATH`, run the Python file using the following command:

      ```bash
      python ./app/kafka_consumer.py
      ```

## Contact
If you have any questions, please feel free to contact me via [sampinval@gmail.com].