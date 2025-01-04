import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm


def rename_s3_folder(bucket_name, old_folder, new_folder):
    """
    Déplace tous les objets de old_folder vers new_folder dans le même bucket S3.

    :param bucket_name: Nom du bucket S3.
    :param old_folder: Préfixe actuel du dossier (doit se terminer par '/').
    :param new_folder: Nouveau préfixe du dossier (doit se terminer par '/').
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    try:
        # Liste tous les objets dans l'ancien dossier
        objects_to_move = list(bucket.objects.filter(Prefix=old_folder))
        total_objects = len(objects_to_move)

        if total_objects == 0:
            print(f"Aucun objet trouvé dans le dossier {old_folder}.")
            return

        print(f"{total_objects} objets trouvés dans le dossier {old_folder}.")

        for obj in tqdm(objects_to_move, desc="Déplacement des objets"):
            old_key = obj.key
            # Définir le nouveau key en remplaçant l'ancien préfixe par le nouveau
            new_key = old_key.replace(old_folder, new_folder, 1)

            # Ignorer si c'est le dossier lui-même (ex: Dexter_history/)
            if old_key == old_folder:
                continue

            # Copier l'objet vers le nouveau préfixe
            bucket.copy({
                'Bucket': bucket_name,
                'Key': old_key
            }, new_key)

            # Supprimer l'objet original
            obj.delete()

        print(f"Tous les objets ont été déplacés de {old_folder} vers {new_folder} avec succès.")

    except ClientError as e:
        print(f"Erreur lors du renommage du dossier : {e}")


if __name__ == "__main__":
    bucket_name = 'phardev'
    old_folder = 'Dexter/Dexter_history/'  # Assurez-vous que le préfixe se termine par '/'
    new_folder = 'Dexter/'  # Assurez-vous que le préfixe se termine par '/'

    rename_s3_folder(bucket_name, old_folder, new_folder)
