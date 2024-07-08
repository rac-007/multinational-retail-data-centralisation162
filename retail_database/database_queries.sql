-- Quering the data: Answering business questions and extracting the data from the database using SQL.
-- Scenario: How many stores does the business have and in which countries?
SELECT country_code, count(store_code) AS total_no_store
	FROM dim_store_details
	GROUP BY (country_code)
	ORDER BY total_no_store DESC;

-- Scenario: Which location currently have the most stores?
SELECT locality, count(store_code) AStotal_no_stores
	FROM dim_store_details
	GROUP BY(locality)
	ORDER BY total_no_stores DESC
	LIMIT 7;

-- Scenario: Which month produced the largest amount of sales?
SELECT 
	dim_date_times.month,
	SUM(dim_products.product_price*orders_table.product_quantity) as total_sales
FROM 
	orders_table
LEFT JOIN
	dim_date_times on orders_table.date_uuid = dim_date_times.date_uuid
LEFT JOIN
	dim_products on orders_table.product_code = dim_products.product_code
GROUP BY
	dim_date_times.month
ORDER BY
	total_sales DESC;
	
-- How many sales are coming from online?
SELECT store_type FROM dim_store_details
	WHERE store_type = 'Web';

--How many sales are happening online vs offline.
--Calculate how many products were sold and the amount of sales made for online and offline purchases
SELECT
	COUNT(orders_table.product_quantity) AS number_of_sales,
	SUM(orders_table.product_quantity) AS product_quantity_count,
	CASE
		WHEN dim_store_details.store_type IS NULL THEN 'Web'
		ELSE 'Offline'
	END AS location
FROM orders_table
LEFT JOIN
	dim_store_details
	ON
	orders_table.store_code = dim_store_details.store_code
GROUP BY
	location
ORDER BY
	product_quantity_count;

-- What percentage of sale come through each type of store?
-- The sales team wants to know which of the different store types has generated the most revenue so they know where to focus.
-- Find out the total and percentage of sales coming from each of the different store types.
SELECT 
	CASE
		WHEN dim_store_details.store_type = 'Local' THEN 'Local'
		WHEN dim_store_details.store_type = 'Super Store' THEN 'Super Store'
		WHEN dim_store_details.store_type = 'Mall Kiosk' THEN 'Mall Kiosk'
		WHEN dim_store_details.store_type = 'Outlet' THEN 'Outlet'
		--WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web Portal'
		ELSE 'Web Portal'
	END AS store_type,
	SUM(orders_table.product_quantity*dim_products.product_price) AS total_sales,
	SUM(orders_table.product_quantity * dim_products.product_price)/ 
	(SELECT SUM(orders_table.product_quantity * dim_products.product_price) FROM orders_table
	 	LEFT JOIN dim_products ON orders_table.product_code = dim_products.product_code)*100 AS percentage_total
FROM orders_table
LEFT JOIN
	dim_products
	ON
	orders_table.product_code = dim_products.product_code
LEFT JOIN
	dim_store_details
	ON
	orders_table.store_code = dim_store_details.store_code
GROUP BY
	store_type
ORDER BY
	total_sales DESC;

-- Find which months in which years have had the most sales historically.
SELECT 
-- In PostgreSQL, you need to explicitly cast the result of the SUM function to a numeric type before using the ROUND function.
    ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS NUMERIC), 2) AS total_sales,
    dim_date_times.year AS year,
    dim_date_times.month AS month
FROM 
    orders_table
LEFT JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
LEFT JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY
    year, month
ORDER BY
    total_sales DESC
LIMIT 10;


-- Determine which type of store is generating the most sales in Germany.
SELECT 
	ROUND(CAST(SUM(dim_products.product_price*orders_table.product_quantity) AS NUMERIC), 2) AS total_sales,
	dim_store_details.store_type AS store_type,
	dim_store_details.country_code AS country_code
FROM 
	orders_table
LEFT JOIN
	dim_products ON orders_table.product_code = dim_products.product_code
LEFT JOIN
	dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE 
    dim_store_details.country_code = 'DE'	
GROUP BY
	dim_store_details.country_code, dim_store_details.store_type
ORDER BY
	total_sales;

--


-- Determine the staff numbers in each of the countries the company sells in.
The query should return the values:
SELECT
	SUM(staff_numbers) AS total_staff_number,
	country_code
From dim_store_details
GROUP BY country_code
ORDER BY total_staff_number DESC;

-- Sales would like to get an accurate metric for how quickly the company is making sales.
-- Determine the average time taken between each sale grouped by year,
-- Use SQL LEAD function.
/*
    Creating multiple temporary tables or views that exists only for the duration of the query.
    Using "WITH" clause to define those temporary tables.
    In order to execute this query, we need a table that contains columns 'year' and 'sale_order_timestamp'
    to apply LEAD function. Out of this table, creating another temporary table 'year_time_diff', with columns
    'year' and 'average_time_diff' to get the desired outcome.
*/
WITH 
date_time_table AS (
    SELECT 
        EXTRACT(hour FROM CAST(timestamp AS time)) AS hour,
        EXTRACT(minute FROM CAST(timestamp AS time)) AS minutes,
        EXTRACT(second FROM CAST(timestamp AS time)) AS seconds,
        day,
        month,
        year,
        date_uuid
    FROM 
        dim_date_times
),
timestamp_table AS (
    SELECT 
        MAKE_TIMESTAMP(year::int, month::int, day::int, hour::int, minutes::int, seconds::float) AS sale_order_timestamp,
        date_uuid,
        year::int AS year
    FROM 
        date_time_table
),
timestamp_diffs AS (
    SELECT 
        year,
        sale_order_timestamp - LEAD(sale_order_timestamp) OVER (PARTITION BY year ORDER BY sale_order_timestamp DESC) AS time_diff
    FROM 
        orders_table
    JOIN 
        timestamp_table ON orders_table.date_uuid = timestamp_table.date_uuid
),
year_time_diffs AS (
    SELECT 
        year,
        AVG(time_diff) AS average_time_diff
    FROM 
        timestamp_diffs
    GROUP BY 
        year
    ORDER BY 
        average_time_diff DESC
)
SELECT 
    year, 
    CONCAT(
        'hours: ', EXTRACT(HOUR FROM average_time_diff), ' ',
        'minutes: ', EXTRACT(MINUTE FROM average_time_diff), ' ',
        'seconds: ', EXTRACT(SECOND FROM average_time_diff), ' ',
        'milliseconds: ', EXTRACT(MILLISECONDS FROM average_time_diff)
    ) AS average_time_taken
FROM 
    year_time_diffs;
--



