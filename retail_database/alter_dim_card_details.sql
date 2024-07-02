-- Working with dim_card_details table
select max(length(cast(card_number as text))) as max_card_number_length,
	   max(length(cast(expiry_date as text))) as max_expiry_date_length,
	   max(length(cast(date_payment_confirmed as text))) as max_date_payment_confirmed_length
from dim_card_details;        

-- Returned Values:
-- max_card_number_length=22
-- max_expiry_date_length=10
-- max_date_payment_confirmed_length=17

ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(22),
    ALTER COLUMN expiry_date TYPE VARCHAR(10),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

--Add Primary Key
ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);