insertion :   CREATE EXTENSION IF NOT EXISTS dblink;

INSERT INTO dim_vendor (vendor_id, name)
SELECT DISTINCT VendorID, 'Vendor ' || VendorID
FROM dblink('dbname=nyc_warehouse',
            'SELECT VendorID FROM public.nyc_raw') AS t(VendorID INT);           
            

-- Insérer les valeurs de PULocationID dans la table dim_location
INSERT INTO dim_location (pulocation_id)
SELECT DISTINCT PULocationID
FROM dblink('dbname=nyc_warehouse',
            'SELECT PULocationID FROM public.nyc_raw') AS t(PULocationID INT);

-- Insérer les valeurs de DOLocationID dans la table dim_location
INSERT INTO dim_location (dolocation_id)
SELECT DISTINCT DOLocationID
FROM dblink('dbname=nyc_warehouse',
            'SELECT DOLocationID FROM public.nyc_raw') AS t(DOLocationID INT);
            
INSERT INTO dim_payment_type (payment_type_id, name)
SELECT DISTINCT payment_type, 'Payment Type ' || payment_type
FROM dblink('dbname=nyc_warehouse',
            'SELECT payment_type FROM public.nyc_raw') AS t(payment_type INT);
            
INSERT INTO fact_trips (
    vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
    ratecode_id, store_and_fwd_flag, pickup_location_id, dropoff_location_id,
    payment_type_id, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee
)
SELECT
    dv.vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    COALESCE(passenger_count, 0) AS passenger_count,
    trip_distance,
    COALESCE(RatecodeID, 0) AS ratecode_id,
    COALESCE(store_and_fwd_flag, 'N') AS store_and_fwd_flag,
    dl_pickup.location_id,
    dl_dropoff.location_id,
    dpt.payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    COALESCE(congestion_surcharge, 0) AS congestion_surcharge,
    COALESCE(airport_fee, 0) AS airport_fee
FROM (
    SELECT
        VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        COALESCE(passenger_count, 0) AS passenger_count,
        trip_distance,
        RatecodeID,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        Airport_fee
    FROM dblink('dbname=nyc_warehouse',
                'SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee FROM public.nyc_raw') AS t(
        VendorID INT,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INT,
        trip_distance FLOAT,
        RatecodeID INT,
        store_and_fwd_flag CHAR(1),
        PULocationID INT,
        DOLocationID INT,
        payment_type INT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        congestion_surcharge FLOAT,
        Airport_fee FLOAT
    )
) AS subquery
INNER JOIN dim_vendor dv ON subquery.VendorID = dv.vendor_id
INNER JOIN dim_location dl_pickup ON subquery.PULocationID = dl_pickup.location_id
INNER JOIN dim_location dl_pickup ON subquery.DOLocationID = dl_pickup.location_id
INNER JOIN dim_payment_type dpt ON subquery.payment_type = dpt.payment_type_id