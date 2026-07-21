CREATE OR REPLACE PROCEDURE sales.sp_merge_dim_customer()
LANGUAGE plpgsql
AS $$
BEGIN

UPDATE sales.dim_customer AS dc
SET record_end_ts=b.record_start_ts-interval '1 second',
    active_flag=0
FROM (
    SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY record_start_ts DESC) AS row_num
            FROM sales.stage_dim_customer
        ) deduped_stage
        WHERE row_num=1
) b
WHERE dc.customer_id=b.customer_id 
    AND dc.active_flag=1
    AND dc.record_end_ts>b.record_start_ts
    AND b.cdc_operation IN ('U','D');

--insert new/changed record
INSERT INTO sales.dim_customer
    (cdc_operation, customer_id, cust_email, cust_phone, cust_address, cust_country,
     cust_city, hash_value, record_start_ts, record_end_ts,
     active_flag, cust_first_name, cust_last_name)
SELECT
    b.cdc_operation, b.customer_id, b.cust_email, b.cust_phone, b.cust_address,
    b.cust_country, b.cust_city, b.hash_value,
    b.record_start_ts, b.record_end_ts, b.active_flag, b.cust_first_name,
    b.cust_last_name
FROM (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY record_start_ts DESC) AS row_num
        FROM sales.stage_dim_customer
    ) deduped_stage
    WHERE row_num = 1
) b
WHERE b.cdc_operation IN ('U','I');

-- clear the staging table
TRUNCATE TABLE sales.stage_dim_customer;

END;
$$;
