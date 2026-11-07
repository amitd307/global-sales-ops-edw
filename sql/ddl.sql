-- Microsoft Fabric Warehouse (T-SQL) DDL for the global EDW.
-- Fabric Warehouse does not enforce PK/FK constraints; keys are documented here
-- and integrity is enforced by the pipeline (reconciliation + unknown members).

CREATE TABLE dbo.dim_date (
    date_key     INT        NOT NULL,      -- yyyymmdd
    [date]       DATE       NOT NULL,
    [year]       INT        NOT NULL,
    [quarter]    INT        NOT NULL,
    [month]      INT        NOT NULL,
    year_month   CHAR(7)    NOT NULL,
    fiscal_year  INT        NOT NULL       -- April-March
);

CREATE TABLE dbo.dim_store (
    store_key    INT          NOT NULL,    -- surrogate, one per SCD2 version
    store_code   VARCHAR(10)  NOT NULL,
    region       CHAR(2)      NOT NULL,    -- IN / JP / IT ...
    country      VARCHAR(50)  NOT NULL,
    city         VARCHAR(50)  NOT NULL,
    [format]     VARCHAR(30)  NOT NULL,
    row_hash     CHAR(64)     NOT NULL,
    valid_from   DATE         NOT NULL,
    valid_to     DATE         NOT NULL,
    is_current   BIT          NOT NULL
);

CREATE TABLE dbo.dim_product (
    product_key  INT          NOT NULL,    -- -1 = Unknown
    product_code VARCHAR(20)  NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category     VARCHAR(50)  NOT NULL
);

CREATE TABLE dbo.fact_sales (
    order_id        VARCHAR(30)   NOT NULL, -- region-prefixed, e.g. JP:JP-0000001
    region          CHAR(2)       NOT NULL,
    date_key        INT           NOT NULL,
    store_key       INT           NOT NULL,
    product_key     INT           NOT NULL,
    quantity        INT           NOT NULL,
    currency        CHAR(3)       NOT NULL,
    amount_local    DECIMAL(18,2) NOT NULL,
    amount_inr      DECIMAL(18,2) NULL,
    amount_usd      DECIMAL(18,2) NULL,
    fulfilment_days INT           NULL
);
