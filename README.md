# Epic EHI Tables Data Dictionary

This project scrapes and organizes the data dictionary from Epic EHI Tables website (https://open.epic.com/EHITables/GetTable/_index.htm).

## Overview

The data dictionary includes table and column information from Epic's Electronic Health Information (EHI) tables, organized alphabetically by the first letter of each table name.

## Contents

- `epic_data_tables/`: Directory containing the extracted data organized by the first letter of table names
- `epic_data_scraper_alphabetical.py`: Main script that automatically scrapes all tables and organizes data by letter
- `page_content_inspector.py`: Utility script to inspect web page structure
- `requirements.txt`: Required Python packages

## Usage

1. Install requirements:
   ```
   pip install -r requirements.txt
   ```

2. Run the script:
   ```
   python epic_data_scraper_alphabetical.py
   ```

The script will:
- Process all tables from the Epic EHI website
- Track progress and can be resumed if interrupted
- Create separate CSV files for each letter in the alphabet
- Save detailed processing information in a summary file

## Data Structure

Each CSV file contains the following columns:
- `table_name`: Name of the table
- `column_name`: Name of the column
- `primary_key`: Whether the column is a primary key (Y/N)
- `ordinal_position`: Position of the column in the table
- `type`: Data type of the column
- `discontinued`: Whether the column is discontinued
- `description`: Description of the column

## Requirements

- Python 3.8+
- Playwright
- Pandas
