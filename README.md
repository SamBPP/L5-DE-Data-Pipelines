# UK Data Pipeline – Learning Project

This repository provides the starter files for a data engineering exercise where learners build a small-scale data pipeline using Python and SQL. The project simulates ingesting and cleaning user data from a UK-based application and storing it into a structured database.

---

## 📦 Project Structure

```
.
├── data/                   # Raw CSV data
├── src/                    # Python modules for ETL
├── notebooks/              # Jupyter notebooks
├── tests/                  # Unit tests
├── docs/                   # Documentation (schema, pipeline steps)
├── .devcontainer/          # Codespaces setup
├── requirements.txt        # Python dependencies
├── README.md               # This file
└── .gitignore              # Files to exclude from Git tracking

````

---

## 🧪 Requirements

This project runs in a Python 3.11+ environment. It has been configured to work out-of-the-box in GitHub Codespaces using the `.devcontainer` folder.

If you're working locally, install dependencies using:

```bash
pip install -r requirements.txt
````

---

## 🚀 How to Run the Pipeline

1. Start a Codespace or clone the repo locally.
2. Load the notebook or run:

```bash
python src/pipeline_runner.py
```

3. This will:

   * Load CSV data
   * Clean and validate the data
   * Insert it into a SQLite database (`user_data.db`)

---

## 📁 Input Data

Located in the `data/` folder:

* `UK_User_Data.csv`: sample UK user profiles
* `UK-User-LoginTS.csv`: user login timestamps (epoch format)

---

## 📚 Learning Objectives

* Understand and apply data cleaning and transformation techniques
* Design and implement a normalized database schema
* Use Python to build a modular and testable ETL pipeline
* Convert real-world edge cases into structured, queryable data
* Practice technical documentation, validation, and soft stakeholder engagement