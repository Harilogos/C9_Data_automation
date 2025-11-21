-- ============================================================
--  Schema Definition for client_dashboard_local
--  Auto-generated from existing PostgreSQL structure
-- ============================================================

CREATE SCHEMA IF NOT EXISTS public;

-- ============================================================
-- 1. Table: discom_bill_v2
-- ============================================================
CREATE TABLE IF NOT EXISTS public.discom_bill_v2 (
    bill_header              TEXT,
    unit                     TEXT,
    month_year               TEXT,
    tariff                   TEXT,
    total_consumption        TEXT,
    cost_without_solar       TEXT,
    cost_with_solar_wheeling TEXT,
    discom_bill              TEXT,
    savings                  TEXT
);

-- ============================================================
-- 2. Table: gen_cons_15min_data_v2
-- ============================================================
CREATE TABLE IF NOT EXISTS public.gen_cons_15min_data_v2 (
    reading_date        DATE,
    reading_time        TIME WITHOUT TIME ZONE,
    location            TEXT,
    unit                TEXT,
    tod_slot            TEXT,
    consumption         NUMERIC,
    supplied_generation NUMERIC
);

-- ============================================================
-- 3. Table: hourly_gen_con2_v2
-- ============================================================
CREATE TABLE IF NOT EXISTS public.hourly_gen_con2_v2 (
    date                DATE NOT NULL,
    time                TIME WITHOUT TIME ZONE NOT NULL,
    unit                TEXT NOT NULL,
    tod_slot            TEXT,
    consumption         NUMERIC(12,4),
    supplied_generation NUMERIC(12,4)
);

-- ============================================================
-- 4. Table: monthly_banking_settlement_data_v2
-- ============================================================
CREATE TABLE IF NOT EXISTS public.monthly_banking_settlement_data_v2 (
    month                            TEXT,
    unit                             TEXT,
    consumption                      NUMERIC,
    supplied_generation              NUMERIC,
    surplus_generation               NUMERIC,
    surplus_demand                   NUMERIC,
    matched_settlement               NUMERIC,
    settlement_with_banking          NUMERIC,
    surplus_generation_after_banking NUMERIC,
    surplus_demand_after_banking     NUMERIC
);

-- ============================================================
-- 5. Table: monthly_savings_v2
-- ============================================================
CREATE TABLE IF NOT EXISTS public.monthly_savings_v2 (
    month                       TEXT,
    unit                        TEXT,
    consumption                 NUMERIC(15,4),
    grid_cost                   NUMERIC(15,4),
    actual_cost_with_banking    NUMERIC(15,4),
    savings_with_banking        NUMERIC(15,4),
    savings_pct_with_banking    NUMERIC(6,2),
    actual_cost_without_banking NUMERIC(15,4),
    savings_without_banking     NUMERIC(15,4),
    savings_pct_without_banking NUMERIC(6,2)
);
