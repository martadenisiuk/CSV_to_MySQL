# CSV to MySQL Importer

## Overview
This script imports CSV files into a MySQL database, automatically creating tables if they don’t exist.
1. Install dependencies:

```shell
pip install requirements.txt
```
2. Configure `config.json` with database credentials and table names.

## Usage
Run the script with:

```shell
python CSVtoMYSQL.py
```


## Features
- Reads CSV files and inserts data into MySQL.
- Creates tables dynamically if they don’t exist.
- SQL does not support storing 'inf' or '-inf' values. According to SQL documentation, these values are not recognized and can cause errors. Therefore, they have been replaced with NULLs