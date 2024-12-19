'''# Task 1 
SELECT 
    country_code, 
    COUNT(*) AS store_count
FROM 
    dim_store_details
WHERE 
    store_type != 'Web Portal'
GROUP BY 
    country_code
ORDER BY 
    store_count DESC

# Task 2 
SELECT 
    locality, 
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    locality
ORDER BY 
    total_no_stores DESC;

# Task 3 
SELECT 
    dim_date_times.month, 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
FROM 
    orders_table
JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid -- Retrieves the month for each order
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code -- Retrieves the price of each product ordered
GROUP BY 
    dim_date_times.month
ORDER BY 
    total_sales DESC;

# Task 4
SELECT
    COUNT(orders_table.index) AS numbers_of_sales, 
    SUM(orders_table.product_quantity) AS product_quantity_count, 
    CASE
        WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    orders_table
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code -- Retrieves the store type
GROUP BY
    location
ORDER BY
	numbers_of_sales

# Task 5
SELECT
    dim_store_details.store_type,
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    ROUND((SUM(orders_table.product_quantity * dim_products.product_price) / 
           (SELECT SUM(orders_table.product_quantity * dim_products.product_price)
            FROM orders_table
            JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
            JOIN dim_products ON orders_table.product_code = dim_products.product_code) * 100), 2) AS "sales_made(%)"
FROM
    orders_table
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY
    dim_store_details.store_type
ORDER BY
    total_sales DESC;

# Task 6
SELECT
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    dim_date_times.year,
    dim_date_times.month
FROM
    orders_table
JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY
    dim_date_times.year, dim_date_times.month
ORDER BY
    total_sales DESC;

# Task 7 
SELECT
    SUM(dim_store_details.staff_numbers) AS total_staff_numbers,
    dim_store_details.country_code
FROM
    dim_store_details
GROUP BY
    dim_store_details.country_code
ORDER BY
    total_staff_numbers DESC;

# Task 8 
SELECT
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    dim_store_details.store_type,
    dim_store_details.country_code
FROM
    orders_table
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
WHERE
    dim_store_details.country_code = 'DE'
GROUP BY
    dim_store_details.store_type, dim_store_details.country_code
ORDER BY
    total_sales;

# Task 9 
WITH sale_differences AS (
    SELECT 
        dim_date_times.year,
        dim_date_times.month,
        dim_date_times.day,
        dim_date_times.timestamp,
        -- Combine year, month, day, and timestamp to create a full timestamp
        TO_TIMESTAMP(
            dim_date_times.year || '-' || LPAD(dim_date_times.month::text, 2, '0') || '-' || LPAD(dim_date_times.day::text, 2, '0') || ' ' || dim_date_times.timestamp,
            'YYYY-MM-DD HH24:MI:SS'
        ) AS sale_time_full,
        -- Use LEAD to get the timestamp of the next sale within the same year, ordered by full timestamp
        LEAD(
            TO_TIMESTAMP(
                dim_date_times.year || '-' || LPAD(dim_date_times.month::text, 2, '0') || '-' || LPAD(dim_date_times.day::text, 2, '0') || ' ' || dim_date_times.timestamp,
                'YYYY-MM-DD HH24:MI:SS'
            )
        ) OVER (PARTITION BY dim_date_times.year ORDER BY 
            TO_TIMESTAMP(
                dim_date_times.year || '-' || LPAD(dim_date_times.month::text, 2, '0') || '-' || LPAD(dim_date_times.day::text, 2, '0') || ' ' || dim_date_times.timestamp,
                'YYYY-MM-DD HH24:MI:SS'
            )
        ) AS next_sale_time_full
    FROM 
        dim_date_times

)
, time_differences AS (
    SELECT 
        year,
        -- Calculate the time difference as an interval between sale_time_full and next_sale_time_full
        (next_sale_time_full - sale_time_full) AS time_difference
    FROM
        sale_differences

)
SELECT 
    year,
    CONCAT(
        '"hours": ', 
        EXTRACT(HOUR FROM AVG(time_difference)),  
        ', "minutes": ', 
        EXTRACT(MINUTE FROM AVG(time_difference)),  
        ', "seconds": ', 
        ROUND(EXTRACT(SECOND FROM AVG(time_difference))),  
        ', "milliseconds": ',
        ROUND(EXTRACT(MILLISECOND FROM AVG(time_difference)))  
    ) AS actual_time_taken
FROM
    time_differences
GROUP BY
    year
ORDER BY
    year DESC;
'''