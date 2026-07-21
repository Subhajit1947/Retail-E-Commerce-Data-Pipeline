-- Create Product Dimension Table
CREATE TABLE sales.dim_product (
    product_sk      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cdc_operation       VARCHAR(50),
    product_id          VARCHAR(64) NOT NULL,
    product_name        VARCHAR(64),
    brand_name          VARCHAR(64),
    product_description TEXT,
    product_price       DOUBLE PRECISION,
    product_category    VARCHAR(64),
    hash_value          VARCHAR(64),
    record_start_ts     TIMESTAMP WITHOUT TIME ZONE,
    record_end_ts       TIMESTAMP WITHOUT TIME ZONE,
    active_flag         INTEGER
);

TRUNCATE TABLE sales.stage_dim_product;

DROP TABLE IF EXISTS sales.stage_dim_product;

CREATE TABLE sales.stage_dim_product (
    stage_customer_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cdc_operation       VARCHAR(50),
    product_id          VARCHAR(64) NOT NULL,
    product_name        VARCHAR(64),
    brand_name          VARCHAR(64),
    product_description TEXT,
    product_price       DOUBLE PRECISION,
    product_category    VARCHAR(64),
    hash_value          VARCHAR(64),
    record_start_ts     TIMESTAMP WITHOUT TIME ZONE,
    record_end_ts       TIMESTAMP WITHOUT TIME ZONE,
    active_flag         INTEGER
);


