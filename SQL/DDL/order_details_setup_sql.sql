-- Create Order Details Fact Table
CREATE TABLE sales.fact_order_details (
    order_details_id VARCHAR(64) NOT NULL,
    order_id          VARCHAR(64),
    product_id        VARCHAR(64),
    product_quantity  BIGINT,
    ingestion_date    DATE,

    PRIMARY KEY (order_details_id)
);

CREATE INDEX idx_fact_order_details_order_id
ON sales.fact_order_details (order_id);

CREATE INDEX idx_fact_order_details_product_id
ON sales.fact_order_details (product_id);


