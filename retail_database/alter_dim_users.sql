-- Workinh with dim_users table.
SELECT MAX(LENGTH(CAST(country_code as TEXT))) AS max_country_code_length
FROM dim_users;

-- Returned value max_country_code_length = 3

-- Alter the table to change data-types
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

-- Adding Primary Key
ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid);
   