# Batch Processing of data using spark and airflow
A project involving data engineering and API development
## Background
This project is about data cleaning,analyzing data and creating KPIs
This project is implemented using **Python3.10** ,**Docker** and **Airflow**.


### Data Cleaning
The following data-cleaning steps are done in this project:
* **filter_ip_address** - Data is filtered for valid ip addresses
* **cast_country_to_capital** - Convert first letter of every capital to capital

## Data Analysis

The following data-analysis steps are done in this project:
* **most-frequent-country** - This step finds out the most frequent country for the current run
* **least-frequnt-country** - This step finds out the least country for the current run
* **distinct_user**- This step finds out the number of distinct users for the current run




## Requirements
* The pipeline runs on airflow.
* Docker is needed to run the project in a containerized manner locally
* Additionally, the project also supports local runs without Docker. To run the project locally a virtual environment with python3.10 is needed.

## Getting Started

* To run the project locally using run the following command:

    ```bash
    docker-compose up 
    ```

* To stop the containers and remove the  containers, networks, volumes, and images created by up. Run the following command
    ```bash
    docker-compose down
    ```
  
* To run the the code on airflow. Run the following commands
    ```bash
    ./airflow_install.sh
    cp dags/processing-dag.py  ~/airflow/dags/
    cp batch_processing/processing.py ~/airflow/dags/
    ```
  Go to http://0.0.0.0:8080/home and trigger the data_processing-dag



To run the project locally without Docker :

*  Create a 3.10 python virtual environment using the below command.
    
    ```bash
    python3.10 -m venv .venv
    source .venv/bin/activate
    pip-sync requirements.txt requirements-local.txt
     ```


* To run tests and do static checks on the code . Run the following command
  ```bash
  ./run_tests.sh
  ```
* To run the data cleaning and analysis components locally. Run the following command 
     ```bash
    python3 batch_processing/processing.py run-local-pipeline --input-path data/raw/input_data.json --output-path data/enriched
     ```


### Project Composition
This entire project is on Docker. Python 3.10-slim base images are used to create our images.

Components of the project structure
* `data` - Contains all raw data and the enriched,cleaned data created by data transformation code.
* `batch-processing` - Contains all the code to clean the data and analyze.
* `dags` - Contains the airflow dag.
* `test` - Contains the test
  * `test_data_processing.py` - Unit test to test the data cleaning and transformation step


### Other python tools used
* `black` - Black is used for code formatting
* `isort` - Isort is used to sort the import statements
* `mypy and pylint` - Thes are used to do static checks on the code and identify code smells