#!/bin/bash
# Airflow needs a home. `~/airflow` is the default, but you can put it
# somewhere else if you prefer (optional)
set -e

export AIRFLOW_HOME=$(pwd)
# export PYTHONPATH=$(pwd)

# Install Airflow using the constraints file
AIRFLOW_VERSION=2.6.3
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.6
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.2.5/constraints-3.6.txt
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt

# configs:
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# The Standalone command will initialise the database, make a user,
# and start all components for you.
AIRFLOW_HOME=$(pwd) PYTHONPATH=$(pwd):${PYTHONPATH} airflow standalone

# Visit localhost:8080 in the browser and use the admin account details
# shown on the terminal to login.
# Enable the example_bash_operator dag in the home page
