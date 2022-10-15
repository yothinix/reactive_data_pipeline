# Reactive Data Pipeline demo

This project is part of the talk "The rise of Dataset and its applications" at Grill the data event.

## System requirements

You need at least [Python 3.8](https://www.python.org/) and [Poetry](https://python-poetry.org/) to install the dependencies

## Setting up

Clone this project
```
git clone 
cd reactive_data_pipeline
```

Install the dependencies
```
poetry install
```

Create a DAGSTER_HOME directory
```
mkdir dagster_home
```

## Start the project

To start the project, You can run dagit a web UI for Dagster
```
export DAGSTER_HOME="$(pwd)/dagster_home" && poetry run dagit -m dagster_repository
```

If you want to enable scheduler and sensor you also need to start the dagster daemon here is the example
```
export DAGSTER_HOME="$(pwd)/dagster_home" && poetry run dagster-daemon run -m dagster_repository
```