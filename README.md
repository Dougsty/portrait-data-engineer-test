# Process documentation

## Environment Tools
- Claude for this documentation and layout
- Copilot for autocomplete and docstring generation and better error handling: Prompt(Better error handling, for this function)
- Ruff formatter for Python code standardization
- Jupyter Notebook for interactive data analysis
- Pandas for data manipulation (chosen due to small dataset size; for larger datasets, Spark would be preferred)

## Disclaimer
I organized the repository to emulate my normal approach to projects. The implementation demonstrates my working methodology within the scope of this assessment, focusing on maintainability and scalability principles.

## Repository Structure
I created a src folder for the main project code, a .env file for environment variables, a utils folder for modular functions, and a notebook folder for data exploration. The pipeline structure contains 3 Python files handling all data due to the small size. For larger datasets, I would segregate by data type (e.g., Pipeline/Prescription with raw, refined, and trusted folders).

## Development Process
1. **Data Exploration**: Initial transformation and exploration using Jupyter notebooks
2. **Architecture Design**: Implementation of a medallion architecture
3. **Data Flow**:
   - **Bronze/Raw Layer**: CSV data from the transient zone with no transformation loaded as raw data
   - **Silver/Trusted Layer**: Data transformed with proper data typing
   - **Gold/Refined Layer**: Aggregations and transformations for analysis

I emulated the PostgreSQL schema as a container from a data lake (due to time constraints and test scope). The complete flow is: CSV → Bronze/Raw → Silver/Trusted (with data typing) → Gold/Refined (aggregated for analysis).

### Proper Project
### Estructure

      project_root/
      ├── src/                # Source code directory containing all functions
      │   ├── pipeline/       # Data transformation logic
      │   └── utils/          # Helper functions and utilities
      ├── notebooks/          # Jupyter notebooks for exploration and analysis
      ├── sample_datasets/    # Sample data directory
      └── .env                # Environment variables (not committed to repository)

### Functions Created
   Connection with postgresdatabase: 
   ```python
      def postgres_connection(
         user: str = os.getenv("POSTGRES_USER"),
         password: str = os.getenv("POSTGRES_PASSWORD"),
         host: str = "localhost",
         port: str = "5432",
         database: str = "healthcare",
      ) -> create_engine:
         """
         Create a connection to the PostgreSQL database.
         Args:
            user (str): PostgreSQL username.
            password (str): PostgreSQL password.
            host (str): PostgreSQL host.
            port (str): PostgreSQL port.
            database (str): PostgreSQL database name.
         Returns:
            engine (create_engine): SQLAlchemy engine object.
         Raises:
            ValueError: If any required parameter is missing.
            Exception: If the connection to the database fails.
         """

         # Validate required parameters
         if not user or not password:
            raise ValueError("Database user and password must be provided.")

         try:
            # Create a connection string
            engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
            
            return engine
         except Exception as e:
            raise Exception(f"Failed to connect to the PostgreSQL database: {e}")
   ```
   Data loading with a possibility of full or incremental ingestion
   ```python
   def load_data(df: pd.DataFrame, table_name: str, schema: str) -> None:
    """
    Send data to PostgreSQL database.

    Args:
        df (pd.DataFrame): DataFrame to be sent.
        table_name (str): Name of the table in the database.
        schema (str): Schema where the table will be created.
    """
    try:
        # Convert DataFrame to SQL and send it to PostgreSQL
        engine = postgres_connection()

        # Create schema if it doesn't exist
        with engine.connect() as connection:
            try:
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                connection.commit()
            except Exception as e:
                raise RuntimeError(f"Error creating schema '{schema}': {e}")

        # Check for incremental or full load
        inspector = inspect(engine)
        if inspector.has_table(table_name, schema=schema):
            first_run = "append"
        else:
            first_run = "replace"

        df["date_insertion"] = pd.to_datetime("now")
        # Send DataFrame to the specified schema and table
        try:
            df.to_sql(table_name, con=engine, schema=schema, if_exists=first_run, index=False)
        except Exception as e:
            raise RuntimeError(f"Error loading data into table '{schema}.{table_name}': {e}")

        print(
            f"=== Data sent to {schema}.{table_name} with {first_run} operation successfully!"
        )
    except Exception as e:
        print(f"An error occurred: {e}")
   ```
# Pipeline Documentation
# ETL Pipeline Documentation

This pipeline processes healthcare data through three stages — **Raw**, **Trusted**, and **Refined** — before orchestrating them with a main ETL script.

## Overview

The pipeline consists of four main components:

1. **`raw.py`** – Loads raw CSV data into PostgreSQL (raw layer)  
2. **`trusted.py`** – Transforms raw data into validated, consistent format (trusted layer)  
3. **`refined.py`** – Applies business logic and analytics-ready features (refined layer)  
4. **`elt.py`** – Orchestrates the full ETL process  

---

## 1. Raw Layer (`raw.py`)

### Purpose
Loads all CSV files from the `sample_datasets` directory into PostgreSQL tables under the **`raw`** schema.

### Key Function
```python
def load_all_raw_data():
    """Load all CSV files into PostgreSQL tables."""
    schema = "raw"
    for dataset in os.listdir(dataset_path):
        if dataset.endswith(".csv"):
            try:
                print(f"=== Loading {dataset}...")
                table_name = dataset.split(".")[0]
                data = pd.read_csv(os.path.join(dataset_path, dataset))
                load_data(df=data, table_name=table_name, schema=schema)
                print(f"--- Successfully loaded {table_name=} into {schema=}")
            except FileNotFoundError as e:
                print(f"Error: File not found - {e}")
            except pd.errors.EmptyDataError as e:
                print(f"Error: Empty data in file {dataset} - {e}")
            except Exception as e:
                print(f"Unexpected error while loading {dataset}: {e}")
```

### Features
- Auto-detects all `.csv` files in the dataset directory
- Uses file names (without extension) as PostgreSQL table names
- Comprehensive error handling for:
  - Missing files
  - Empty datasets
  - Unexpected failures

---

## 2. Trusted Layer (`trusted.py`)

### Purpose
Transforms raw data into a validated format stored in the **`trusted`** schema.

### Key Functions
```python
def transform_appointment(df: pd.DataFrame) -> pd.DataFrame:
    # Converts appointment_date to datetime

def transform_patients(df: pd.DataFrame) -> pd.DataFrame:
    # Converts registration_date to datetime and age to integer

def transform_prescriptions(df: pd.DataFrame) -> pd.DataFrame:
    # Converts prescription_date to datetime
```

### Features
- Standardizes and cleans raw data
- Validates and converts date fields
- Converts data types (e.g., integers, datetime)
- Applies table-specific transformation logic
- Maintains data integrity with error handling

---

## 3. Refined Layer (`refined.py`)

### Purpose
Applies business rules and computes derived analytical fields for the **`refined`** schema.

### Key Functions
```python
def refinement_appointment(df: pd.DataFrame) -> pd.DataFrame:
    # Adds day_of_week and days_since_last_appointment

def refinement_patients(df: pd.DataFrame) -> pd.DataFrame:
    # Creates age groups and patient type categories

def refinement_prescriptions(df: pd.DataFrame) -> pd.DataFrame:
    # Adds medication categories and prescription frequency types
```

### Analytical Features Created

**Appointments**
- `day_of_week`
- `days_since_last_appointment`

**Patients**
- Age groups (0–18, 19–30, etc.)
- Months since registration
- Patient type (New, Regular, Old)

**Prescriptions**
- Medication categories (e.g., Pain Relief, Diabetes)
- Prescription frequency (First-time, Repeat)
- Total frequency count per patient

---

## 4. ETL Orchestration (`elt.py`)

### Purpose
Coordinates the execution of the entire pipeline.

### Key Function
```python
def main():
    print("Loading raw data...")
    load_all_raw_data()

    print("Loading trusted data...")
    transform_all_data()

    print("Loading refined data...")
    refine_all_data()
```

### Error Handling
Robust error handling is implemented across the pipeline:
- File not found
- Empty data files
- Missing or malformed columns
- Type conversion issues
- Database connection failures

---

## Data Flow

```text
CSV files 
   ↓ (raw.py)
Raw schema 
   ↓ (trusted.py)
Trusted schema 
   ↓ (refined.py)
Refined schema
```

---

## Usage

Run the entire ETL pipeline with:

```bash
python elt.py
```


# Data Analysis Documentation

## Overview
This section documents the analysis conducted on the healthcare dataset using SQL queries against the refined data layer. The analysis focuses on understanding patient demographics, appointment patterns, and prescription behaviors to derive actionable insights for healthcare operations.

## Setup and Configuration
The analysis leverages the PostgreSQL connection established in the earlier sections of the pipeline, utilizing the refined data layer that contains properly transformed and cleaned data.

```python
import sys
sys.path.append('../../')
from src.utils.connection import postgres_connection
import pandas as pd
```

## Patient Analysis

### Patient Age Distribution
**Question**: What is the distribution of patients across age groups?

**SQL Query**:
```sql
SELECT 
    age_group, 
    COUNT(*) as patient_distribution
FROM 
    healthcare.refined.patients
GROUP BY 
    age_group
```
**Results**: 
| Age Group | Patient Distribution |
|-----------|----------------------|
| 0-18      | 1                    |
| 19-30     | 8                    |
| 31-50     | 14                   |
| 51-70     | 12                   |
| 71+       | 15                   |

### Appointment Frequency by Patient Type
**Question**: How does the appointment frequency vary by patient type?

**SQL Query**:
```sql
SELECT 
    p.patient_type,
    COUNT(p.patient_id) as qtt_patients_by_type
FROM 
    refined.patients p
LEFT JOIN 
    refined.appointments a ON a.patient_id = p.patient_id
GROUP BY 
    p.patient_type
```

**Results**: Regulars are the most frequent patients, who are within the 24-month registration interval.
| Patient Type | Patient Count |
|-------------|---------------|
| Old         | 49            |
| Regular     | 57            |
| New         | 6             |

**Business Implication**: Regular patients represent an important demographic for the healthcare facility, indicating that retention strategies are working effectively. Further analysis could explore conversion rates from new to regular patients.

## Appointment Analysis

### Common Appointment Types by Age Group
**Question**: What are the most common appointment types by age group?

**SQL Query**:
```sql
SELECT 
    a.appointment_type,
    p.age_group,
    COUNT(*) as qtt_patients_by_type
FROM 
    refined.patients p
LEFT JOIN 
    refined.appointments a ON a.patient_id = p.patient_id
GROUP BY 
    a.appointment_type, p.age_group 
ORDER BY 
    qtt_patients_by_type DESC
```

**Results**: Checkup is the most common one
| Appointment Type | Age Group | Patient Count |
|------------------|-----------|---------------|
| Checkup          | 51-70     | 14            |
| Checkup          | 71+       | 13            |
| Consultation     | 31-50     | 10            |
| Emergency        | 71+       | 9             |
| Emergency        | 31-50     | 8             |
| Emergency        | 19-30     | 8             |
| Checkup          | 31-50     | 7             |
| Emergency        | 51-70     | 7             |
| Consultation     | 51-70     | 6             |
| Consultation     | 19-30     | 6             |
| Checkup          | 19-30     | 5             |
| Consultation     | 71+       | 5             |

**Business Implication**: The prevalence of checkups suggests an opportunity to enhance preventive care programs, potentially leading to better health outcomes and reduced emergency visits.

### Emergency Visits by Day of Week
**Question**: Are there specific days of the week with higher emergency visits?

**SQL Query**:
```sql
SELECT 
    a.appointment_type,
    a.day_of_week,
    p.age_group,
    COUNT(*) as qtt_patients_by_type
FROM 
    refined.patients p
LEFT JOIN 
    refined.appointments a ON a.patient_id = p.patient_id
WHERE 
    appointment_type = 'Emergency'
GROUP BY 
    a.appointment_type, a.day_of_week, p.age_group
ORDER BY 
    qtt_patients_by_type DESC
```

**Results**: 
Friday shows the highest number of emergency visits. This temporal pattern could be related to work-week stress, delayed care-seeking behavior, or staffing patterns.

| Appointment Type | Day of Week | Age Group | Count |
|------------------|-------------|-----------|-------|
| Emergency        | Friday      | 31-50     | 4     |
| Emergency        | Friday      | 19-30     | 2     |
| Emergency        | Saturday    | 71+       | 2     |
| Emergency        | Saturday    | 51-70     | 2     |
| Emergency        | Monday      | 31-50     | 2     |
| Emergency        | Friday      | 51-70     | 2     |
| Emergency        | Thursday    | 19-30     | 2     |
| Emergency        | Saturday    | 19-30     | 2     |
| Emergency        | Tuesday     | 71+       | 2     |
| Emergency        | Monday      | 19-30     | 2     |

**Business Implication**: The facility should consider adjusting staffing levels for emergency services on Fridays to accommodate the higher demand. Further investigation into the causes of this pattern could help develop preventive interventions.

## Prescription Analysis

### Most Prescribed Medication Categories by Age Group
**Question**: What are the most prescribed medication categories by age group?

**SQL Query**:
```sql
SELECT 
    p.medication_category,
    p2.age_group,
    COUNT(*) as qtd_prescription_by_age
FROM 
    refined.prescriptions p 
LEFT JOIN 
    refined.patients p2 ON p2.patient_id = p.patient_id
GROUP BY 
    p.medication_category, p2.age_group
ORDER BY 
    qtd_prescription_by_age DESC
```

**Results**: 
Heart medications are most frequently prescribed for patients in the 31-50 age group.

| Medication Category | Age Group | Prescription Count |
|--------------------|-----------|-------------------|
| Heart              | 31-50     | 16                |
| Pain Relief        | 71+       | 15                |
| Pain Relief        | 51-70     | 15                |
| Heart              | 71+       | 13                |
| Heart              | 51-70     | 10                |
| Diabetes           | 71+       | 10                |
| Pain Relief        | 31-50     | 10                |
| Antibiotic         | 71+       | 7                 |
| Pain Relief        | 19-30     | 5                 |
| Antibiotic         | 51-70     | 5                 |

**Business Implication**: This insight could inform targeted health education programs for cardiovascular health in middle-aged patients. It also suggests a need for preventive cardiology services.

### Prescription and Appointment Frequency Correlation
**Question**: How does prescription frequency correlate with appointment frequency?

**SQL Query**:
```sql
WITH prescription_freq AS (
    SELECT 
        p.patient_id,
        p.prescription_frequency
    FROM 
        refined.prescriptions p 
    LEFT JOIN 
        refined.patients p2 ON p2.patient_id = p.patient_id
),
appointment_freq AS (
    SELECT 
        patient_id,
        COUNT(*) as appointment_frequency
    FROM 
        refined.prescriptions p 
    GROUP BY 
        patient_id
)
SELECT 
    pf1.patient_id,
    prescription_frequency,
    appointment_frequency,
    prescription_frequency = appointment_frequency as correlation
FROM 
    prescription_freq pf1
LEFT JOIN 
    appointment_freq pf2 ON pf2.patient_id = pf1.patient_id
```

      # Python analysis of correlation results
      correlation_summary = prescription_appointment_correlation.groupby("correlation").size().reset_index(name="count")

| Correlation | Count |
|-------------|-------|
| True        | 128   |

**Results**: 
The analysis confirms a strong correlation between appointment and prescription frequencies. This suggests that prescriptions are consistently issued during consultations, with each patient visit typically resulting in a prescription.

**Business Implication**: This pattern indicates consistent prescription practices across providers. Further analysis could examine prescription appropriateness and explore opportunities for medication management programs.

## Conclusion

The data analysis reveals several key insights about the healthcare facility's operations:

1. **Patient Demographics**: The age distribution shows balanced care across different life stages, with notable concentrations in specific age groups.

2. **Appointment Patterns**: Regular patients form the core of the facility's business, with checkups being the most common appointment type. Emergency visits spike on Fridays.

3. **Prescription Behavior**: Heart medications dominate prescriptions for middle-aged patients, and there's a strong correlation between appointments and prescriptions.

These findings can inform operational decisions, resource allocation, and targeted healthcare programs to improve patient outcomes and facility efficiency.

## Next Steps

Based on the analysis, potential next steps include:

1. Developing targeted preventive care programs for high-risk groups
2. Optimizing staffing for peak emergency periods (especially Fridays)
3. Implementing medication management initiatives for frequently prescribed categories
4. Further exploring the demographic patterns to enhance service delivery

## Technical Implementation Notes

The analysis was implemented using:
- PostgreSQL for database storage and query execution
- Pandas for data manipulation and result processing
- Python for orchestrating the analysis workflow
