création :CREATE TABLE dim_vendor (
    vendor_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    pulocation_id INT,
    dolocation_id INT
);

-- Supprimer la clé primaire existante
ALTER TABLE public.dim_location
DROP CONSTRAINT dim_location_pkey;

-- Ajouter une nouvelle clé primaire sur pulocation_id et dolocation_id
ALTER TABLE public.dim_location
ADD CONSTRAINT dim_location_pkey PRIMARY KEY (pulocation_id, dolocation_id);

CREATE TABLE dim_payment_type (
    payment_type_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE fact_trips (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INT REFERENCES dim_vendor(vendor_id),
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INT NOT NULL,
    trip_distance FLOAT NOT NULL,
    ratecode_id INT NOT NULL,
    store_and_fwd_flag CHAR(1) NOT NULL,
    pickup_location_id INT REFERENCES dim_location(location_id),
    dropoff_location_id INT REFERENCES dim_location(location_id),
    payment_type_id INT REFERENCES dim_payment_type(payment_type_id),
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT
);