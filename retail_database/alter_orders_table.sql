-- Working with orders_table
/*
 The CAST function in SQL is used to convert a value from one data type to another.
 The LENGTH function, which calculates the length of a string, requires its 
 argument to be of a string type.
*/
-- Find out the maximum lengths defining the VARCHAR columns
select max(length(cast(card_number as text))) as max_card_number_length,
	   max(length(cast(store_code as text))) as max_store_code_length,
	   max(length(cast(product_code as text))) as max_product_code_length	
from orders_table;
-- Following values are returned:
-- max_card_number_length = 19
-- max_store_code_length = 12
-- max_product_code_length = 11

-- Alter the table to change data-types

ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;


-- Following error occured when adding Foreign Key with 'user_uuid'    
-- ERROR:  Key (user_uuid)=(6904f151-6d32-4d1e-b477-41801a9a8e83) is not present in table "dim_users".insert or update on table "orders_table" violates foreign key constraint "orders_table_user_uuid_fkey" 

-- ERROR:  insert or update on table "orders_table" violates foreign key constraint "orders_table_user_uuid_fkey"
-- SQL state: 23503
-- Detail: Key (user_uuid)=(6904f151-6d32-4d1e-b477-41801a9a8e83) is not present in table "dim_users".
-- To resolve above error, finding and inserting those values into din_users table.
-- Finds all user_uuid in orders_table that are not in dim_users.
SELECT orders_table.user_uuid
FROM orders_table
LEFT JOIN dim_users
ON orders_table.user_uuid = dim_users.user_uuid
WHERE dim_users.user_uuid IS NULL;

-- Inserts all user_uuid from orders_tale not present in dim_card_details initally, into dim_users.
INSERT INTO dim_users(user_uuid)
SELECT DISTINCT orders_table.user_uuid
FROM orders_table
WHERE orders_table.user_uuid NOT IN 
	(SELECT dim_users.user_uuid
	FROM dim_users);    

-- Adding Foreign Key.
ALTER TABLE orders_table
    ADD FOREIGN KEY (user_uuid)
    REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
    ADD FOREIGN KEY (store_code)
    REFERENCES dim_store_details(store_code);


