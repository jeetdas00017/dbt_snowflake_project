
create schema Warehouse;

CREATE OR REPLACE TABLE Warehouse.SNAP_CUSTOMERS (
    DBT_SCD_ID           VARCHAR,
    DBT_UPDATED_AT       TIMESTAMP,
    DBT_VALID_FROM       TIMESTAMP,
    DBT_VALID_TO         TIMESTAMP,
    CUSTOMER_ID          NUMBER,
    FIRST_NAME           VARCHAR,
    LAST_NAME            VARCHAR,
    EMAIL                VARCHAR,
    PHONE                VARCHAR,
    ADDRESS              VARCHAR,
    CITY                 VARCHAR,
    STATE                VARCHAR,
    COUNTRY              VARCHAR,
    ACQUISITION_CHANNEL  VARCHAR,
    SIGNUP_DATE          DATE
);

CREATE OR REPLACE TABLE Warehouse.SNAP_PRODUCTS (
    DBT_SCD_ID       VARCHAR,
    DBT_UPDATED_AT   TIMESTAMP,
    DBT_VALID_FROM   TIMESTAMP,
    DBT_VALID_TO     TIMESTAMP,
    PRODUCT_ID       NUMBER,
    PRODUCT_NAME     VARCHAR,
    CATEGORY         VARCHAR,
    SUB_CATEGORY     VARCHAR,
    BRAND            VARCHAR,
    PRICE            NUMBER(18,2),
    COST             NUMBER(18,2)
);


/* ============================================================
   DIMENSIONS
============================================================ */

CREATE OR REPLACE TABLE Warehouse.DIM_CUSTOMERS (
    CUSTOMER_SK          VARCHAR,
    CUSTOMER_ID          NUMBER,
    FIRST_NAME           VARCHAR,
    LAST_NAME            VARCHAR,
    EMAIL                VARCHAR,
    PHONE                VARCHAR,
    ADDRESS              VARCHAR,
    CITY                 VARCHAR,
    STATE                VARCHAR,
    COUNTRY              VARCHAR,
    ACQUISITION_CHANNEL  VARCHAR,
    SIGNUP_DATE          DATE,
    EFFECTIVE_FROM       TIMESTAMP,
    EFFECTIVE_TO         TIMESTAMP,
    IS_CURRENT           BOOLEAN
);

CREATE OR REPLACE TABLE Warehouse.DIM_PRODUCTS (
    PRODUCT_SK       VARCHAR,
    PRODUCT_ID       NUMBER,
    PRODUCT_NAME     VARCHAR,
    CATEGORY         VARCHAR,
    SUB_CATEGORY     VARCHAR,
    BRAND            VARCHAR,
    PRICE            NUMBER(18,2),
    COST             NUMBER(18,2),
    EFFECTIVE_FROM   TIMESTAMP,
    EFFECTIVE_TO     TIMESTAMP,
    IS_CURRENT       BOOLEAN
);


/* ============================================================
   FACTS
============================================================ */

CREATE OR REPLACE TABLE Warehouse.FACT_ORDERS (
    ORDER_ID         NUMBER,
    CUSTOMER_SK      VARCHAR,
    PRODUCT_SK       VARCHAR,
    CUSTOMER_ID      NUMBER,
    PRODUCT_ID       NUMBER,
    ORDER_DATE       TIMESTAMP,
    QUANTITY         NUMBER,
    UNIT_PRICE       NUMBER(18,2),
    DISCOUNT         NUMBER(18,2),
    TOTAL_AMOUNT     NUMBER(18,2),
    ORDER_STATUS     VARCHAR,
    PAYMENT_METHOD   VARCHAR,
    CREATED_AT       TIMESTAMP,
    UPDATED_AT       TIMESTAMP
);

ALTER TABLE Warehouse.FACT_ORDERS
ADD PRIMARY KEY (ORDER_ID);


/* ============================================================
   SALES MART
============================================================ */

CREATE OR REPLACE TABLE Warehouse.CUSTOMER_LIFETIME_VALUE (
    CUSTOMER_ID           NUMBER,
    FIRST_NAME            VARCHAR,
    LAST_NAME             VARCHAR,
    EMAIL                 VARCHAR,
    COUNTRY               VARCHAR,
    ACQUISITION_CHANNEL   VARCHAR,
    TOTAL_ORDERS          NUMBER,
    LIFETIME_VALUE        NUMBER(18,2),
    FIRST_ORDER_DATE      TIMESTAMP,
    LAST_ORDER_DATE       TIMESTAMP,
    AVG_ORDER_VALUE       NUMBER(18,2)
);

CREATE OR REPLACE TABLE Warehouse.SALES_PERFORMANCE (
    ORDER_DATE          DATE,
    CATEGORY            VARCHAR,
    SUB_CATEGORY        VARCHAR,
    BRAND               VARCHAR,
    COUNTRY             VARCHAR,
    STATE               VARCHAR,
    TOTAL_UNITS_SOLD    NUMBER,
    TOTAL_REVENUE       NUMBER(18,2),
    TOTAL_DISCOUNT      NUMBER(18,2),
    TOTAL_ORDERS        NUMBER,
    UNIQUE_CUSTOMERS    NUMBER
);


/* ============================================================
   MARKETING MART
============================================================ */

CREATE OR REPLACE TABLE Warehouse.CUSTOMER_ACQUISITION (
    ACQUISITION_CHANNEL   VARCHAR,
    TOTAL_CUSTOMERS       NUMBER,
    TOTAL_ORDERS          NUMBER,
    TOTAL_REVENUE         NUMBER(18,2),
    REVENUE_PER_CUSTOMER  NUMBER(18,2),
    CONVERSION_RATE       FLOAT
);

CREATE OR REPLACE TABLE Warehouse.CUSTOMER_RFM_SEGMENTATION (
    CUSTOMER_ID           NUMBER,
    EMAIL                 VARCHAR,
    ACQUISITION_CHANNEL   VARCHAR,
    COUNTRY               VARCHAR,
    SIGNUP_DATE           DATE,
    LAST_ORDER_DATE       TIMESTAMP,
    RECENCY_DAYS          NUMBER,
    FREQUENCY             NUMBER,
    MONETARY              NUMBER(18,2),
    RECENCY_SCORE         NUMBER,
    FREQUENCY_SCORE       NUMBER,
    MONETARY_SCORE        NUMBER,
    RFM_TOTAL_SCORE       NUMBER,
    CUSTOMER_SEGMENT      VARCHAR
);
