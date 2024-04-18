# Data Ranger Proof of Concept (POC)

This repository contains the necessary Docker configuration to set up an Airflow environment for the Data Ranger POC.

# Description

This project implements an automated system for processing CSV files using Apache Airflow, storing the processed data in a PostgreSQL database and performing statistical calculations on the data. This is designed for both batch and real-time data processing.

![etl](https://github.com/manolobkno08/data-ranger-poc/assets/11824356/ebdd8f6a-2ece-457a-8eae-d32cac33504e)


## Functionality

- **CSV file processing**: Airflow's DAG (Directed Acyclic Graph) is configured to process CSV files sequentially. Each file is read, processed, and its data is inserted into the database.

- **Statistics calculation**: During the processing of each file, statistics such as total rows, average, minimum, and maximum price are calculated. These statistics are also stored in the database.

- **Real-time update**: Each insertion in the database updates the statistics in real time without needing to reprocess all data.

## Prerequisites

Before you begin, ensure you have met the following requirements:
* You have installed Docker and Docker Compose.
* You have basic understanding of Docker containerization.

## Getting Started

Follow these steps to get your Airflow environment up and running:

1. **Initial Airflow Setup**

Initialize the Airflow database, create an admin user, and upgrade the db to the latest version: `docker-compose up airflow-init`

2. **Start Airflow**

Launch Airflow services in detached mode: `docker-compose up -d`

3. **Webserver Login**

Access the Airflow webserver at `http://localhost:8080`. Use the following credentials to log in:

- **Username:** airflow
- **Password:** airflow

## Troubleshooting

If you encounter issues, you might need to reset your environment:

- **Stop and Remove Containers**

To stop and remove all Docker containers, networks, and volumes created run: `docker-compose down -v`

- **Rebuild and Restart**

If you have added new libraries or changed the Docker configuration, rebuild the images and restart the services: `docker-compose up -d --build`


## Additional Information

* This setup is configured to run Airflow with the Local Executor.
* For production environments, further configuration is needed.





