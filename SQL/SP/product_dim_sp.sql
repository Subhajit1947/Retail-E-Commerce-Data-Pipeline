CREATE OR REPLACE PROCEDURE sales.sp_merge_dim_product()
LANGUAGE plpgsql
AS $$
BEGIN

UPDATE sales.dim_product AS dp
SET record_end_ts=b.record_start_ts-interval '1 second',
    active_falg=0
FROM (
    WITH deduped_stage AS (
        SELECT *,ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY record_start_ts DESC) AS row_num
        FROM sales.stage_dim_product
    )
    SELECT *
    FROM deduped_stage
    WHERE row_num=1
) b
WHERE dp.product_id=b.product_id
    AND dp.active_flag=1
    AND dp.record_end_ts>b.record_start_ts
    AND b.cdc_operation IN ('D','U');


-- Insert records into dim_product for all I and U records from the deduplicated table
INSERT INTO sales.dim_product
   (cdc_operation, product_id, product_name, brand_name ,product_description, product_category, 
    product_price, hash_value, record_start_ts, record_end_ts, 
    active_flag) 
SELECT
    deduped.cdc_operation, deduped.product_id, deduped.product_name, deduped.brand_name, deduped.product_description, 
    deduped.product_category, deduped.product_price, deduped.hash_value, 
    deduped.record_start_ts, deduped.record_end_ts, deduped.active_flag
FROM (
    WITH deduped_stage AS (
        SELECT *,ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY record_start_ts DESC) AS row_num
        FROM sales.stage_dim_product
    )
    SELECT *
    FROM deduped_stage
    WHERE row_num=1
) b
WHERE b.cdc_operation IN ('I','U');


TRUNCATE TABLE sales.stage_dim_product;

END;
$$;






