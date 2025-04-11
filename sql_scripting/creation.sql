-- Dimension Tables

-- Dimension for Vendors
CREATE TABLE dim_vendor (
    vendor_key SERIAL PRIMARY KEY,
    vendor_id INT UNIQUE, -- Business key from source
    vendor_name VARCHAR(255) -- Placeholder for actual name if available
);

-- Dimension for Rate Codes
CREATE TABLE dim_rate_code (
    rate_code_key SERIAL PRIMARY KEY,
    rate_code_id INT UNIQUE, -- Business key from source
    rate_code_name VARCHAR(255) -- e.g., 'Standard rate', 'JFK', etc.
);

-- Dimension for Payment Types
CREATE TABLE dim_payment_type (
    payment_type_key SERIAL PRIMARY KEY,
    payment_type_id INT UNIQUE, -- Business key from source
    payment_type_name VARCHAR(255) -- e.g., 'Credit card', 'Cash', etc.
);

-- Dimension for Locations (simplified, needs enrichment from external lookup table)
CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    location_id INT UNIQUE, -- Business key from source (PULocationID/DOLocationID)
    borough VARCHAR(255), -- Placeholder, requires external data
    zone VARCHAR(255), -- Placeholder, requires external data
    service_zone VARCHAR(255) -- Placeholder, requires external data
);

-- Dimension for Dates
CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL, -- (0=Sunday, 6=Saturday) or (1=Monday, 7=Sunday) depending on locale/preference
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);


-- Fact Table

CREATE TABLE fact_trips (
    trip_id BIGSERIAL PRIMARY KEY, -- Surrogate key for the fact record

    -- Foreign Keys to Dimensions
    vendor_key INT REFERENCES dim_vendor(vendor_key),
    pickup_date_key INT REFERENCES dim_date(date_key),
    dropoff_date_key INT REFERENCES dim_date(date_key),
    pickup_location_key INT REFERENCES dim_location(location_key),
    dropoff_location_key INT REFERENCES dim_location(location_key),
    rate_code_key INT REFERENCES dim_rate_code(rate_code_key),
    payment_type_key INT REFERENCES dim_payment_type(payment_type_key),

    -- Degenerate Dimension(s)
    store_and_fwd_flag TEXT,

    -- Precise Timestamps
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,

    -- Measures
    passenger_count INT,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    trip_duration INTERVAL -- Calculated: dropoff - pickup
);

-- Indexing (Optional but recommended for performance)
CREATE INDEX idx_fact_trips_pickup_date ON fact_trips(pickup_date_key);
CREATE INDEX idx_fact_trips_dropoff_date ON fact_trips(dropoff_date_key);
CREATE INDEX idx_fact_trips_pickup_loc ON fact_trips(pickup_location_key);
CREATE INDEX idx_fact_trips_dropoff_loc ON fact_trips(dropoff_location_key);
CREATE INDEX idx_fact_trips_payment_type ON fact_trips(payment_type_key);
CREATE INDEX idx_fact_trips_rate_code ON fact_trips(rate_code_key);

CREATE EXTENSION IF NOT EXISTS dblink;
