from minio import Minio
import urllib.request
import os
import sys


def main():
    grab_data()

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method downloads a file of the New York Yellow Taxi.

    Files need to be saved into "../../data/raw" folder
    This method takes no arguments and returns nothing.
    """
    month = int(input("Veuillez entrer le mois  : "))

    # Construit le lien en fonction du mois choisi
    file_link = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{month:02d}.parquet"

    # Dossier de destination
    destination_folder = "raw"

    # Vérifie si le dossier de destination existe, sinon le crée
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # Télécharge le fichier
    file_name = os.path.join(destination_folder, os.path.basename(file_link))
    urllib.request.urlretrieve(file_link, file_name)
    print(f"Le fichier {file_name} a été téléchargé avec succès.")

    # Met à jour la fonction pour utiliser la variable 'month' plutôt que de demander à l'utilisateur à nouveau
    write_data_minio(file_name)


def write_data_minio(file_path):
    """
    This method puts the Parquet file into Minio
    """
    client = Minio(
        "localhost:9000",  # Assurez-vous que le port est correct ici
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = "yellowtaxi"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"Le bucket {bucket} a été créé avec succès.")

    # Upload du fichier dans le bucket
    object_name = os.path.basename(file_path)
    client.fput_object(bucket, object_name, file_path)
    print(f"Le fichier {object_name} a été uploadé dans le bucket {bucket}")


if __name__ == '__main__':
    sys.exit(main())
