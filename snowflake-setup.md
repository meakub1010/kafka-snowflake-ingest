# Snowflake Setup Guide: Kafka Snowflake Ingestion

This guide explains how to **create the Snowflake warehouse, database, schema, tables, streams, tasks, and views** for Kafka ingestion.

---

## Prerequisites

* Snowflake account with appropriate privileges
* SnowSQL CLI installed or access via Snowflake Web UI
* Basic familiarity with Snowflake SQL

---

## Step 1: Create Warehouse

```sql
CREATE OR REPLACE WAREHOUSE KAFKA_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
```

* **AUTO_SUSPEND = 300** → warehouse suspends after 5 minutes of inactivity
* **AUTO_RESUME = TRUE** → warehouse automatically resumes when a query runs

---

## Step 2: Create Database

```sql
CREATE OR REPLACE DATABASE KAFKA_DB;
```

> ⚠ `CREATE OR REPLACE DATABASE` will drop the existing database if it exists. Use with caution in production.

---

## Step 3: Create Schema

```sql
CREATE OR REPLACE SCHEMA KAFKA_DB.KAFKA_SCHEMA;
```

* Organizes tables, streams, and views within the database.

---

## Step 4: Set Context

Ensure you are using the correct warehouse, database, and schema:

```sql
USE WAREHOUSE KAFKA_WH;
USE DATABASE KAFKA_DB;
USE SCHEMA KAFKA_DB.KAFKA_SCHEMA;
```

---

## Step 5: Create Tables

### Staging Table

```sql
CREATE OR REPLACE TABLE TRANSACTIONS_STAGING (
    ID VARCHAR(16777216),
    TRADE_DATE TIMESTAMP_NTZ(9),
    TRADER VARCHAR(16777216),
    FUND VARCHAR(16777216),
    SECURITY VARCHAR(16777216),
    QUANTITY NUMBER(38,0),
    PRICE NUMBER(38,9)
);
```

### Main Transactions Table

```sql
CREATE OR REPLACE TABLE TRANSACTIONS (
    ID VARCHAR(16777216),
    TRADE_DATE TIMESTAMP_NTZ(9),
    TRADER VARCHAR(16777216),
    FUND VARCHAR(16777216),
    SECURITY VARCHAR(16777216),
    QUANTITY NUMBER(38,0),
    PRICE NUMBER(38,9),
    TOTAL_VALUE NUMBER(38,9),
    INSERT_TS TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);
```

* `TOTAL_VALUE` = `PRICE * QUANTITY`
* `INSERT_TS` automatically stores insert timestamp

---

## Step 6: Create Stream

Tracks changes in the staging table:

```sql
CREATE OR REPLACE STREAM TRANSACTIONS_STAGING_STREAM
ON TABLE TRANSACTIONS_STAGING APPEND_ONLY = TRUE;
```

---

## Step 7: Create Task

Load data from staging into main table automatically:

```sql
CREATE OR REPLACE TASK LOAD_TRANSACTIONS_TASK
    WAREHOUSE = KAFKA_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('TRANSACTIONS_STAGING_STREAM')
AS
INSERT INTO TRANSACTIONS (
    id, trade_date, trader, fund, security, quantity, price, total_value
)
SELECT
    id,
    trade_date,
    INITCAP(trader),
    INITCAP(fund),
    UPPER(security),
    quantity,
    price,
    price * quantity
FROM TRANSACTIONS_STAGING_STREAM
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY trade_date DESC) = 1;
```

> ⚠ **Remember to resume the task** after creation:

```sql
ALTER TASK LOAD_TRANSACTIONS_TASK RESUME;
```

---

## Step 8: Create Views

### 1. Accountant Market Value

```sql
CREATE OR REPLACE VIEW ACCOUNTANT_MARKET_VALUE (
    FUND,
    TRADER,
    TOTAL_MARKET_VALUE
) AS
SELECT
    fund,
    trader,
    SUM(price * quantity) AS total_market_value
FROM TRANSACTIONS
GROUP BY fund, trader;
```

### 2. Trader Positions

```sql
CREATE OR REPLACE VIEW TRADER_POSITIONS (
    TRADER,
    FUND,
    SECURITY,
    TOTAL_QUANTITY,
    AVG_PRICE,
    POSITION_VALUE
) AS
SELECT
    trader,
    fund,
    security,
    SUM(quantity) AS total_quantity,
    AVG(price) AS avg_price,
    SUM(price * quantity) AS position_value
FROM TRANSACTIONS
GROUP BY trader, fund, security;
```

### 3. Investor Rate of Return (ROR)

```sql
CREATE OR REPLACE VIEW INVESTOR_ROR (
    FUND,
    SECURITY,
    START_VALUE,
    END_VALUE,
    RATE_OF_RETURN
) AS
WITH daily_snapshot AS (
    SELECT
        trade_date::DATE AS snapshot_date,
        fund,
        security,
        SUM(price * quantity) AS market_value
    FROM TRANSACTIONS
    GROUP BY trade_date::DATE, fund, security
),
start_end AS (
    SELECT
        fund,
        security,
        FIRST_VALUE(market_value) OVER (PARTITION BY fund, security ORDER BY snapshot_date) AS start_value,
        LAST_VALUE(market_value) OVER (PARTITION BY fund, security ORDER BY snapshot_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_value
    FROM daily_snapshot
)
SELECT
    fund,
    security,
    start_value,
    end_value,
    CASE
        WHEN start_value = 0 THEN 0
        ELSE  (end_value - start_value) / start_value
    END AS rate_of_return
FROM start_end
GROUP BY fund, security, start_value, end_value;
```

---

## Step 9: Verification

1. Check warehouse status:

```sql
SHOW WAREHOUSES LIKE 'KAFKA_WH';
```

2. Verify tables:

```sql
SHOW TABLES IN SCHEMA KAFKA_DB.KAFKA_SCHEMA;
```

3. Verify streams:

```sql
SHOW STREAMS IN SCHEMA KAFKA_DB.KAFKA_SCHEMA;
```

4. Verify tasks:

```sql
SHOW TASKS IN SCHEMA KAFKA_DB.KAFKA_SCHEMA;
```

5. Verify views:

```sql
SHOW VIEWS IN SCHEMA KAFKA_DB.KAFKA_SCHEMA;
```

---

## Notes / Best Practices

* `CREATE OR REPLACE` will **drop existing objects** — use cautiously in production.
* For recurring runs, consider **truncating staging tables** instead of replacing:

```sql
TRUNCATE TABLE TRANSACTIONS_STAGING;
```

* Resume tasks after creation to enable automatic processing.
* Always **set the correct warehouse** before running queries.
