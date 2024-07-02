-- Working with dim_date_times table
select max(length(cast(month as text))) as max_month_length,
	   max(length(cast(year as text))) as max_year_length,
	   max(length(cast(day as text))) as max_day_length,
       max(length(cast(time_period as text))) as max_time_period_length,
	   max(length(cast(date_uuid as text))) as max_date_uuid_length
from dim_date_times;

-- Returned values
-- max_month_length =2
-- max_year_length =4
-- max_day_length =2
-- max_time_period_length =10
-- max_date_uuid_length = 36

ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(2),
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

--Add Primary Key
ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid);