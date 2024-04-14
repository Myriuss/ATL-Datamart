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
new_db_name = "test"

# Requête pour vérifier si la table existe déjà
check_table_query = sql.SQL(
    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = {})").format(
    sql.Literal('test_dim'))

# Requête pour créer la table table_taxi dans la nouvelle base de données
create_table_query = """
CREATE TABLE IF NOT EXISTS dimension_frais (
    fare_amount DECIMAL(10,2),
    extra DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    airport_fee DECIMAL(10,2)
);
"""

# Requête pour insérer les données de nyc_warehouse.table_taxi dans new_bd.table_taxi
insert_data_query = """
INSERT INTO dimension_frais (fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee)
SELECT DISTINCT fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
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
        cur_destination.execute(
            "INSERT INTO dimension_frais (fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            row  # Inserer les valeurs de la ligne directement
        )

    print("Données insérées avec succès dans la nouvelle base de données.")

except psycopg2.Error as e:
    print("Erreur lors de la connexion à la base de données PostgreSQL:", e)
