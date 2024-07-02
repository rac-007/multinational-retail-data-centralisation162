-- Working with dim_products table.
-- Removing £ sign from each product_price column.

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '')
;
ALTER TABLE dim_products
    ADD COLUMN weight_class VARCHAR;

-- Adds weight categories in weight_class based on product weight
UPDATE dim_products
SET weight_class = 
    CASE 
        WHEN weight < 2.0 THEN 'Light'
        WHEN weight >= 2.0 AND weight < 40.0 THEN 'Mid_sized'
        WHEN weight >= 40.0 AND weight < 140.0 THEN 'Heavy'
        WHEN weight >= 140.0 THEN 'Truck_required'
    END;    

-- Detrmine the maximum lengths of the columns.

SELECT MAX(LENGTH(CAST(product_price AS TEXT))) AS max_product_price_length,
       MAX(LENGTH(CAST("EAN" AS TEXT))) AS max_EAN_length,
       MAX(LENGTH(CAST(product_code AS TEXT))) AS max_product_code_length,
       MAX(LENGTH(CAST(weight_class AS TEXT))) AS max_weight_class_length
FROM dim_products;    

-- Returned results:
--max_product_price_length = 6
--max_EAN_length = 17
--max_product_code_length = 11
--max_weight_class_length = 14

--  Rename column name from 'removed' to 'still_available'
ALTER TABLE dim_products
    RENAME COLUMN removed to still_available;     

--  Alter the table to change data-types
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
    ALTER COLUMN "EAN" TYPE VARCHAR(17),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
    ALTER COLUMN still_available TYPE boolean USING (still_available='Still_available'), 
    ALTER COLUMN weight_class TYPE VARCHAR(14);

--Add Primary Key
ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);