# Data Pipeline Documentation

## Environment Tools
- Claude for this documentation organization and layout
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

