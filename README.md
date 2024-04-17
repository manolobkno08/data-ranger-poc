# Data Ranger Proof of Concept (POC)

This repository contains the necessary Docker configuration to set up an Airflow environment for the Data Ranger POC.

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

Thank you for using Data Ranger POC Airflow environment!





