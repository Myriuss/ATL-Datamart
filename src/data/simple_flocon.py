import psycopg2
from psycopg2 import sql

# Paramètres de connexion à la base de données
db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
}

# Paramètres pour la nouvelle base de données
new_db_name = "nyc_datamart"

# Requête pour vérifier si la table existe déjà
check_table_query = sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = {})").format(sql.Literal('table_taxi'))

# Requête pour créer la table table_taxi dans la nouvelle base de données
create_table_query = """
CREATE TABLE IF NOT EXISTS table_taxi (
    VendorID int,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count float,
    trip_distance float,
    RatecodeID float,
    store_and_fwd_flag varchar,
    PULocationID int,
    DOLocationID int,
    payment_type int,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    improvement_surcharge float,
    total_amount float,
    congestion_surcharge float,
    Airport_fee float
);
CREATE TABLE IF NOT EXISTS date_dim (
    date_id serial PRIMARY KEY,
    date date,
    day_of_week int,
    month int,
    year int
);
"""

# Requête pour insérer les données de nyc_warehouse.table_taxi dans new_bd.table_taxi
insert_data_query = """
INSERT INTO table_taxi (
    VendorID, 
    tpep_pickup_datetime, 
    tpep_dropoff_datetime, 
    passenger_count, 
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
)
SELECT
    VendorID, 
    tpep_pickup_datetime, 
    tpep_dropoff_datetime, 
    passenger_count, 
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
FROM 
   nyc_warehouse.public.nyc_raw;
   
INSERT INTO date_dim (date, day_of_week, month, year)
SELECT DISTINCT
    tpep_pickup_datetime::date AS date,
    EXTRACT(DOW FROM tpep_pickup_datetime) AS day_of_week,
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS month,
    EXTRACT(YEAR FROM tpep_pickup_datetime) AS year
FROM
    nyc_warehouse.public.nyc_raw;

"""


try:
    # Connexion à la base de données nyc_warehouse
    conn_source = psycopg2.connect(
        dbname=db_config["dbms_database"],
        user=db_config["dbms_username"],
        password=db_config["dbms_password"],
        host=db_config["dbms_ip"],
        port=db_config["dbms_port"]
    )
    conn_source.autocommit = True
    cur_source = conn_source.cursor()

    # Récupérer les données de la table nyc_raw
    cur_source.execute("SELECT * FROM public.nyc_raw")
    rows = cur_source.fetchall()

    # Connexion à la nouvelle base de données nyc_datamart
    conn_destination = psycopg2.connect(
        dbname=new_db_name,
        user=db_config["dbms_username"],
        password=db_config["dbms_password"],
        host=db_config["dbms_ip"],
        port=db_config["dbms_port"]
    )
    conn_destination.autocommit = True
    cur_destination = conn_destination.cursor()

    # Créer la table table_taxi dans la nouvelle base de données
    cur_destination.execute(create_table_query)

    # Insérer les données dans la nouvelle table
    for row in rows:
        cur_destination.execute("INSERT INTO table_taxi VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)



    print("Données insérées avec succès dans la nouvelle base de données.")

except psycopg2.Error as e:
    print("Erreur lors de la connexion à la base de données PostgreSQL:", e)