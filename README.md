# 🎯 Spark Pipeline Industrialization: A Packaged Python Project

------------------------------------------------------------------------
[![CI/CD - Run Databricks Tests](https://github.com/BaptisteBell1/sales_pipeline_TP/actions/workflows/ci.yml/badge.svg)](https://github.com/BaptisteBell1/sales_pipeline_TP/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Databricks](https://img.shields.io/badge/Databricks-FF3621.svg?logo=databricks&logoColor=white)

## 🧱 Project Overview

This project focuses on **industrializing** the initial data processing workflow by transitioning from exploratory notebooks to a **structured and packaged Python application**.

Designed around a **Medallion Architecture** (Bronze -> Silver -> Gold), this pipeline aims to be fully reproducible, testable, and maintainable. It ensures a seamless flow from raw data ingestion to refined analytics, supporting automated execution and long-term versioning.

The Gold layer structures the data to answer specific business questions:

* **Global Performance**: Consolidated monthly revenue (normalized to a single currency).
* **Store Performance**: Comparative analysis of revenue across different boutiques.
* **Best Sellers (Volume)**: Identification of most popular products by units sold.
* **Best Sellers (Value)**: Identification of highest-grossing products.

------------------------------------------------------------------------

## 🗂️ Project structure

    sales_pipeline_TP/
    │
    ├── sales_pipeline/                 # Code source du projet
    │   ├── config/             
    │   │    └── config.yaml            # Configuration files
    │   ├── bronze/
    │   │    └── ingestion.py           # Data ingestion
    │   ├── silver/
    │   │    └── cleaning.py            # Data cleaning and filtering
    │   ├── gold/
    │   │    └── aggregation.py         # Data aggregation
    │   └── utils/
    │        └── spark_session.py       # Spark session management
    │        └── utils.py               # Project constants & settings
    │        └── Reset_Raw_Data.ipynb   # Reinitializes project state
    ├── tests/                    
    │   └── test_cleaning.py            # Tests file
    │
    ├── main.py                         # Main pipeline
    ├── pyproject.toml                  # Project configuration
    ├── requirements.txt                # Python dependencies
    └── README.md                       # Project Documentation

------------------------------------------------------------------------
## 🚀 Usage

#### 📦 Install dependencies

Install all dependencies listed in `requirements.txt`:

```bash
pip install -r requirements.txt
```

#### ▶️ Run the main program

To execute the main script:
```bash
python main.py
````

#### 🧪 Run tests

Tests are located in the `tests/` directory. To run them with **pytest**:

```bash
pytest tests/
```
or run the Notebook `tests/run_test.ipynb`

------------------------------------------------------------------------
## 🚀 CI/CD Pipeline: Databricks Integration

To ensure the reliability of the code in the production environment, this project implements a **Continuous Integration (CI)** pipeline using **GitHub Actions** orchestrated directly with **Databricks**.

Instead of running tests on a standard runner, the pipeline triggers a remote execution on the Spark cluster.

### Workflow Logic
Triggered automatically on every push to `main`, the workflow performs the following steps:

#### 🧪 Continuous Integration (CI) : Databricks Validation

1.  **Setup**: Installs the `databricks-cli` and authenticates using secure secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`).
2.  **Remote Trigger**: Initiates the specific Databricks Job (using `Job ID`) responsible for running the test suite.
3.  **Monitoring**: The workflow enters a polling loop, querying the job status via the Databricks API.
4.  **Validation**:
    - ✅ **Pass**: If the remote job returns a `SUCCESS` state.
    - ❌ **Fail**: If the remote job fails, breaking the CI pipeline to prevent bad code deployment.

#### 📦 Continuous Deployment (CD): PyPI Release
Once the Databricks tests pass successfully (`SUCCESS` state), the pipeline automatically triggers the Deployment Job:

5. **Build**: The project is packaged into a standard distributable format (Wheel `.whl` and Source `.tar.gz`) using the Python build backend.
6. **Publish**: The artifacts are securely uploaded to PyPI (Python Package Index).

The workflow uses strict dependency logic (`needs: databricks-tests`), ensuring that broken code is never published.
