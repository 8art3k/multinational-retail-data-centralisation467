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

### Data Transformation and Column Type Casting
## Initial Data Cleaning:
Initially, Python was used to clean and cast the columns of various tables (such as dim_products, dim_users, etc.) to their correct data types. However, it was noticed that once the data was uploaded back into the database, the database reverted to the old data types. Therefore, starting from task 4, these transformations were completed directly within SQL using pgAdmin4.

## Task Highlights:
Columns were casted to their appropriate types, including:
String to Numeric conversion for product prices.
UUID conversion for UUID columns.
Date conversions for date-related columns.
Removed unwanted characters from columns such as product_price (i.e., stripping the currency symbol) and other non-numeric values.
## Adding Primary Keys:
Each table prefixed with dim_ (such as dim_users, dim_products, etc.) was updated to ensure it has a primary key.
These primary keys now match the corresponding columns in the orders_table, which serves as the single source of truth for all orders.
## Creating Primary Key References:
Using SQL, the respective columns in the dim_ tables were updated to become primary key columns.
Foreign Keys were then added to the orders_table to reference the primary keys of the dim_ tables.
## Data Cleanup for Foreign Key Relationships:
While adding foreign key constraints, rows with data missing in the dim_ tables had to be deleted from the orders_table to avoid errors.

### Aim:
The aim of the project is to streamline the process of data cleaning and make it reusable across various data sets with a structured and automated pipeline.

### What I learned:
- How to handle and clean data using Python libraries like pandas.
- How to interact with an S3 bucket, perform transformations, and then load the cleaned data to a relational database like PostgreSQL.
- How to structure data cleaning pipelines using object-oriented principles.
- How to maintain data integrity by adding primary and foreign keys, ensuring consistency across relational tables in a star-based schema.
