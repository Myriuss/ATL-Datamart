insertion2:  -- Insérer les valeurs de DOLocationID dans la table dim_location
INSERT INTO dim_location (location_id)
SELECT DISTINCT DOLocationID
FROM dblink('dbname=nyc_warehouse',
            'SELECT DOLocationID FROM public.nyc_raw WHERE DOLocationID IS NOT NULL') AS t(DOLocationID INT)
WHERE NOT EXISTS (SELECT 1 FROM dim_location WHERE location_id = t.DOLocationID);

-- Insérer les données dans la table fact_trips
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
                'SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee FROM public.nyc_raw WHERE DOLocationID IS NOT NULL') AS t(
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
INNER JOIN dim_location dl_dropoff ON subquery.DOLocationID = dl_dropoff.location_id
INNER JOIN dim_payment_type dpt ON subquery.payment_type = dpt.payment_type_id;