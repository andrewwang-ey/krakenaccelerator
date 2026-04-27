# Kraken-Accelerator

## Description
Kraken Accelerator tool used to move relevant billing data from Energy Companies into the Kraken system. Does so by calling the Kraken API endpoint, the tool will allow the user to perform the migration in different cohorts and automatically map the cohorts through agentic AI. 

## Installation
Run the file run_setup.bat.
    - Installs Python 
    - Installs DBT 

## Usage 
Run file run_app.bat - will open up a GUI created using tkinter. Refer to Features tab for capabilities.

## Important files 
pipeline.py
    - core orchestration script that coordinates the entire data transformation workflow. executes everything 

dbt: 4 models 
landing_data 
    Purpose: Load raw CSV from file path provided at runtime
    Output: All rows and columns, no filtering

validated_data
    Purpose: Data quality check — split valid from invalid records
    Uses: required_fields from mapping YAML
    Output: Two tables (valid + rejected) with rejection reasons tracked


mapped_data
    Rename fields from CSV to Kraken API names
    Apply value transformations 
    Apply cohort filters 

output_data
    Purpose: Final column selection using Kraken API field names
    Output: Ready-to-load JSON for Kraken API


## Features 
Generate Mappings
    - User to upload the flat CSV file containing relevant information
    - Click generate mappings. This matches the CSV's columns to the Kraken schema under kraken-schema.json for API inputs
        -> I might move this part to after the cohorts are determined to have it become more accurate. When user defines cohort provides more metadata and context - right now very inaccurate. 

Edit Mappings
    - Lists out .yml files which relate to API input. Users will go through all to check for mistakes. Will display confidence level of AI agent mapping, user to go through.

Cohort Plan
    - User able to upload csv file outlining all of the relevant cohorts for the migrations. Outlining the number, description, and what 
    - Lists out all of the cohorts, user can run each cohort individually, mark complete, skip, edit and delete
    - Running a cohort runs the file pipeline.py which performs all of the dbt transformations to get into the correct format. 

Run Pipeling
    - Similar to Cohort plan, but for individual inputs. This may get scrapped. I believe cohort plan is much more relevant 

Load to Kraken
    - Loads all of the inputs into Kraken

