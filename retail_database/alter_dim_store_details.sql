--Working with dim_store_details table

SELECT max(LENGTH(CAST(store_code AS TEXT))) AS max_store_code_length,
	  max(LENGTH(CAST(country_code AS TEXT))) AS max_country_code_length
FROM dim_store_details;



-- Drop the 'lat' column as it's no longer needed
ALTER TABLE dim_store_details DROP COLUMN lat;

ALTER TABLE dim_store_details 
    ALTER COLUMN longitude TYPE FLOAT USING longitude:: FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(11),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING  staff_numbers::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,  
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);


-- Add Primary Key
ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);
