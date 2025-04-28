# Setup Guide for Two Circles ETL

This guide provides detailed step-by-step instructions for setting up and running the Two Circles ETL pipeline.

## Prerequisites Installation

### 1. Install Docker and Docker Compose

The Astronomer platform runs on Docker, so you'll need Docker and Docker Compose installed.

**For Windows:**
- Download and install Docker Desktop from [here](https://www.docker.com/products/docker-desktop)
- Docker Desktop includes Docker Compose

**For Mac:**
- Download and install Docker Desktop from [here](https://www.docker.com/products/docker-desktop)
- Docker Desktop includes Docker Compose

**For Linux:**
- Follow the installation instructions for your distribution [here](https://docs.docker.com/engine/install/)
- Install Docker Compose separately by following [these instructions](https://docs.docker.com/compose/install/)

### 2. Install Astronomer CLI

- Instructions for installing Astro CLI for both Mac and Windows (https://www.astronomer.io/docs/astro/cli/install-cli/)

**For Windows:**

```bash
winget install -e --id Astronomer.Astro
```

**For Mac:**

```bash
brew install astro
```

Verify the installation:

```bash
astro version
```

## Project Setup

### 1. Clone the repository

Link to Github repository: https://github.com/Sharjeel9182/two_circles_db_project

```bash
git clone https://github.com/Sharjeel9182/two_circles_db_project.git
```

### 2. Start Airflow

From the project root directory:

```bash
astro dev start
```

This command:
- Builds the Docker image with all dependencies
- Creates and starts containers for each Airflow component
- Mounts your local code to the containers

The first time you run this, it may take a few minutes to download and build everything.

### 3. Access the Airflow UI

- Open your browser and go to [http://localhost:8080]
- Login with the default credentials:
  - Username: `admin`
  - Password: `admin`

## Running the ETL Pipeline

1. Navigate to the DAGs page in the Airflow UI
2. Find the `two_circles_data_pipeline` DAG
3. Trigger the DAG by clicking the "play" button (▶️)
4. Monitor the execution in the "Graph" or "Grid" view

