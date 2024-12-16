import os
import subprocess
import boto3
from docker import from_env as docker_from_env
from dotenv import load_dotenv

load_dotenv()

repository_name = 'winpharma_api'
lambda_names = ['20b1ef4767d6160386bb4daa8ffac8f2', '817c337a912b93c2195e80283e59bfdb',
                    '21b301d285606a9740e6ba8551e8d2a7', '904208ae52f690d61e342a4841764304',
                    '865ecc0d8dd4330a1e342a4841764304', '21ae4d39476607068123b91bdf385fc1',
                    'b30cc7edf420d76708034557df8638ba', '873ffb64f1921335f145c2e01644eeef',
                    '01e5ead18aad2271195e80283e59bfdb', '21b301d285606a9740e6ba8551e8d2a7']

ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name='eu-west-3'  # Optionnel : spécifiez la région ici
)

ecr_client = session.client('ecr', region_name='eu-west-3')
image_tag = f"430054308525.dkr.ecr.eu-west-3.amazonaws.com/{repository_name}:latest"

docker_client = docker_from_env()
image, build_log = docker_client.images.build(path=os.path.dirname(os.path.abspath(__file__)), tag=image_tag, rm=True)
for line in build_log:
    print(line)
# Obtenir le mot de passe d'authentification ECR
password = subprocess.run(['aws', 'ecr', 'get-login-password', '--region', 'eu-west-3'],
                          stdout=subprocess.PIPE ).stdout.decode().strip()
# Authentification Docker
subprocess.run(['docker', 'login', '--username', 'AWS', '--password', password, '430054308525.dkr.ecr.eu-west-3.amazonaws.com'])

# Vérifier si le dépôt existe, sinon le créer
try:
    ecr_client.describe_repositories(repositoryNames=[repository_name])
    print(f"Repository {repository_name} already exists.")
except:
    print(f"Repository {repository_name} not found. Creating it.")
    ecr_client.create_repository(repositoryName=repository_name)


print(docker_client.images.push(image_tag))
print(f"Image pushed: {image_tag}")

lambda_client = session.client('lambda', region_name='eu-west-3')
for lambda_name in lambda_names:
    try:
        response = lambda_client.update_function_code(FunctionName=lambda_name, ImageUri=image_tag)
    except Exception as e :
        if e.response['Error']['Code'] == 'ResourceNotFoundException':

            # Créer une nouvelle Lambda avec l'image ECR
            response = lambda_client.create_function(
                FunctionName=lambda_name,
                Role='arn:aws:iam::430054308525:role/Winpharma_Lambda',
                Code={'ImageUri': image_tag},
                PackageType='Image',
                Timeout=60,  # Timeout en secondes
                MemorySize=128,  # Taille de la mémoire en MB
            )
        else:
            raise e