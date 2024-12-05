# Data Pipeline for Multinational Retail Data Centralisation

## Project Description

This project aims to clean and process sales and order data from multiple sources before uploading it to a database. The pipeline handles:
- Extracting data from rds table, pdf file & json file inside of S3 bucket, and from AWS API.
- Cleaning and preprocessing the data by removing null values, converting weight units, handling time periods, and removing invalid data.
- Uploading the cleaned data into a PostgreSQL database.

### What the project does:
- **Extract**: The data is extracted from multiple sources.
- **Clean**: Several cleaning functions are applied, such as:
  - Removing rows with invalid values.
  - Standardizing column data (e.g., removing non-numeric characters from certain columns).
  - Converting date formats and handling missing values.
- **Upload**: The cleaned data is uploaded to a PostgreSQL database.

### Aim:
The aim of the project is to streamline the process of data cleaning and make it reusable across various data sets with a structured and automated pipeline.

### What I learned:
- How to handle and clean data using Python libraries like pandas.
- How to interact with an S3 bucket, perform transformations, and then load the cleaned data to a relational database like PostgreSQL.
- How to structure data cleaning pipelines using object-oriented principles.
