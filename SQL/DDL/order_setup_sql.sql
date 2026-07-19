CREATE TABLE sales.fact_orders (
    order_id        VARCHAR(64) NOT NULL,
    order_customer_id  VARCHAR(64),
    order_date         DATE,
    order_status       VARCHAR(64),
    payment_method     VARCHAR(64),
    order_platform     VARCHAR(64),
    order_year         INTEGER,
    order_month        INTEGER,
    ingestion_date     DATE,

    PRIMARY KEY (order_id)
);

CREATE INDEX idx_fact_orders_order_date
ON sales.fact_orders (order_date);

CREATE INDEX idx_fact_orders_customer_id
ON sales_uk.fact_orders (order_customer_id);

