Documentation for ETL Pipeline with DBT (GCP)

Overview
This documentation describes the components and functionality of an ETL pipeline integrated with DBT models, which is designed to extract, transform, and load data while also supporting detailed data analysis using SQL joins and aggregations. The ETL pipeline consists of the following stages:
    1. Extract: Retrieves raw data from the source.
    2. Transform: Processes the data to make it analysis-ready.
    3. Load: Loads the processed data into a target database.
Each stage can run independently via dedicated scripts (extract.py, transform.py, and load.py) or collectively through a main driver script (main.py).
Additionally, the pipeline uses DBT to perform advanced data manipulations, including joins and aggregations, on three main tables: users, carts, and products.

Key Scripts and Their Functions
    1. main.py
        ◦ Orchestrates the entire ETL process.
        ◦ Executes extract.py, transform.py, and load.py sequentially.
        ◦ Logs all activities and errors for monitoring.
    2. extract.py
        ◦ Extracts raw data from the source systems or files.
    3. transform.py
        ◦ Cleans and processes raw data.
        ◦ Prepares data for loading into the target database.
    4. load.py
        ◦ Loads transformed data into the database.
    5. Requirements
        ◦ The requirements.txt file includes necessary dependencies:
            ▪ requests
            ▪ psycopg2-binary
            ▪ python-dotenv
            ▪ pandas

DBT (Cleo_gcp)
The DBT project, named Cleo_gcp is used for complex data transformations and joins. DBT models are stored in the models directory.
Tables and Their Columns
    1. carts:
        ◦ cart_id
        ◦ user_id
        ◦ product_id
        ◦ quantity
        ◦ price
        ◦ total_cart_value
    2. products:
        ◦ product_id
        ◦ price
        ◦ name
        ◦ category
        ◦ brand
    3. users:
        ◦ user_id
        ◦ age
        ◦ last_name
        ◦ gender
        ◦ postal_code
        ◦ street
        ◦ city
        ◦ first_name
SQL Queries for Joins and Aggregations
    1. Users and Carts:
        ◦ Purpose: Combine demographic data with transaction details.
This is represented by the model: 
savana_project/models/users_with_carts_model.sql
       SELECT
           u.user_id,
           u.age,
           u.last_name,
           u.gender,
           u.postal_code,
           u.street,
           u.city,
           u.first_name,
           c.cart_id,
           c.product_id,
           c.quantity,
           c.price,
           c.total_cart_value
       FROM
           users u
       INNER JOIN
           carts c ON u.user_id = c.user_id;
    2. 
    3. Carts and Products:
        ◦ Purpose: Enrich transaction data with product details. This is represented by the model:
           savana_project/models/carts_products_details.sql
       SELECT
           c.cart_id,
           c.user_id,
           c.product_id,
           c.quantity,
           c.price,
           c.total_cart_value,
           p.name AS product_name,
           p.category,
           p.brand
       FROM
           carts c
       INNER JOIN
           products p ON c.product_id = p.product_id;
Generated Datasets
    1. User Summary:
        ◦ Fields: user_id, first_name, total_spent, total_items, age, city
        ◦ Insights: Total spending and number of purchases per user.
This is represented by the model: 
savana_project/models/user_summary.sql
       SELECT
           u.user_id,
           u.first_name,
           SUM(c.total_cart_value) AS total_spent,
           SUM(c.quantity) AS total_items,
           u.age,
           u.city
       FROM
           users u
       LEFT JOIN
           carts c ON u.user_id = c.user_id
       GROUP BY
           u.user_id, u.first_name, u.age, u.city;
    2. Category Summary:
        ◦ Fields: category, total_sales, total_items_sold
        ◦ Insights: Aggregate sales by product category.
This is represented by the dbt model: 
savana_project/models/category_summary.sql
       SELECT
           p.category,
           SUM(c.quantity * c.price) AS total_sales,
           SUM(c.quantity) AS total_items_sold
       FROM
           carts c
       INNER JOIN
           products p ON c.product_id = p.product_id
       GROUP BY
           p.category;
    3. Cart Details:
        ◦ Fields: cart_id, user_id, product_id, quantity, price, total_cart_value
        ◦ Insights: Transaction-level details enriched with user and product data.
          Represented by this model:
           savana_project/models/cart_details.sql
       SELECT
           c.cart_id,
           c.user_id,
           c.product_id,
           c.quantity,
           c.price,
           c.total_cart_value,
           u.first_name,
           u.last_name,
           u.city,
           p.name AS product_name,
           p.category,
           p.brand
       FROM
           carts c
       INNER JOIN
           users u ON c.user_id = u.user_id
       INNER JOIN
           products p ON c.product_id = p.product_id;

Notes
    • The main.py script is ideal for running the entire pipeline with a single command.
    • Each ETL stage (extract.py, transform.py, load.py) can be executed separately if needed.
    • DBT models provide flexibility for advanced data transformations and aggregations.
