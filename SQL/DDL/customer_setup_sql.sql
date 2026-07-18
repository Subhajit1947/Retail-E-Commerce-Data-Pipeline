
-----------CREATE CUSTOMER DIMENSION TABLE--------------------------
CREATE TABLE sales.dim_customer (
    cdc_operation VARCHAR(50),
    customer_id VARCHAR(64) NOT NULL,
    cust_email VARCHAR(64),
    cust_phone VARCHAR(64),
    cust_address VARCHAR(65535),
    cust_country VARCHAR(64),
    cust_city VARCHAR(64),
    hash_value VARCHAR(64),
    record_start_ts TIMESTAMP WITHOUT TIME ZONE,
    record_end_ts TIMESTAMP WITHOUT TIME ZONE,
    active_flag INTEGER,
    cust_first_name VARCHAR(64),
    cust_last_name VARCHAR(64),

    PRIMARY KEY (customer_id)
);

CREATE UNLOGGED TABLE sales.stage_dim_customer (
    cdc_operation     VARCHAR(50),
    customer_id       VARCHAR(64) NOT NULL,
    cust_email        VARCHAR(64),
    cust_phone        VARCHAR(64),
    cust_address      TEXT,
    cust_country      VARCHAR(64),
    cust_city         VARCHAR(64),
    hash_value        VARCHAR(64),
    record_start_ts   TIMESTAMP,
    record_end_ts     TIMESTAMP,
    active_flag       INTEGER,
    cust_first_name   VARCHAR(64),
    cust_last_name    VARCHAR(64)
);
 
CREATE INDEX idx_stage_dim_customer_customer_id
    ON sales.stage_dim_customer (customer_id);

