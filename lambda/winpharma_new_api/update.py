import os
import subprocess

import boto3
from docker import from_env as docker_from_env
from dotenv import load_dotenv

load_dotenv()

repository_name = 'winpharma_new_api'
lambda_names = ['winpharma_new_api_test']

# Utiliser la configuration AWS par défaut (celle que tu viens de configurer)
session = boto3.Session(region_name='eu-west-3')

# Optionnel : utiliser les variables d'environnement si elles existent
access_key = os.environ.get('AWS_ACCESS_KEY_ID')
secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

if access_key and secret_key:
    print(f"🔑 Using environment variables for AWS auth")
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='eu-west-3'
    )
else:
    print(f"🔑 Using default AWS configuration")
    session = boto3.Session(region_name='eu-west-3')

ecr_client = session.client('ecr', region_name='eu-west-3')
image_tag = f"430054308525.dkr.ecr.eu-west-3.amazonaws.com/{repository_name}:latest"

# ============================================================================
# BUILD DOCKER AVEC BUILDX (compatible Lambda)
# ============================================================================

print(f"🏗️ Building Docker image for AWS Lambda (x86_64)...")

# Nettoyer les images existantes
try:
    subprocess.run(['docker', 'rmi', image_tag], capture_output=True)
    print(f"🧹 Removed existing image")
except:
    pass

# Build avec docker buildx (compatible Lambda)
build_cmd = [
    'docker', 'buildx', 'build',
    '--platform', 'linux/amd64',
    '--load',  # Important pour charger l'image localement
    '-t', image_tag,
    '.'
]

print(f"🔨 Running: {' '.join(build_cmd)}")
result = subprocess.run(build_cmd, cwd=os.path.dirname(os.path.abspath(__file__)))

if result.returncode != 0:
    print(f"❌ Docker build failed")
    exit(1)

print(f"✅ Docker build successful")

# Vérifier l'architecture
print(f"🔍 Verifying image architecture...")
inspect_result = subprocess.run(
    ['docker', 'inspect', image_tag, '--format={{.Architecture}}'],
    capture_output=True, text=True
)

if inspect_result.returncode == 0:
    arch = inspect_result.stdout.strip()
    print(f"📋 Image architecture: {arch}")
    if arch != "amd64":
        print(f"⚠️ WARNING: Architecture is {arch}, Lambda expects amd64")
else:
    print(f"⚠️ Could not verify architecture")

# ============================================================================
# ECR LOGIN ET PUSH
# ============================================================================

print(f"🔐 Getting ECR login token...")
password_result = subprocess.run(
    ['aws', 'ecr', 'get-login-password', '--region', 'eu-west-3'],
    capture_output=True, text=True
)

if password_result.returncode != 0:
    print(f"❌ Failed to get ECR login token")
    exit(1)

password = password_result.stdout.strip()

print(f"🐳 Logging into ECR...")
login_result = subprocess.run([
    'docker', 'login', '--username', 'AWS', '--password', password,
    '430054308525.dkr.ecr.eu-west-3.amazonaws.com'
], capture_output=True)

if login_result.returncode != 0:
    print(f"❌ Docker login failed")
    exit(1)

print(f"✅ Docker login successful")

# Vérifier si le dépôt existe, sinon le créer
try:
    ecr_client.describe_repositories(repositoryNames=[repository_name])
    print(f"✅ Repository {repository_name} already exists.")
except:
    print(f"🔄 Repository {repository_name} not found. Creating it.")
    ecr_client.create_repository(repositoryName=repository_name)
    print(f"✅ Repository {repository_name} created.")

print(f"📤 Pushing image to ECR...")
push_result = subprocess.run(['docker', 'push', image_tag])

if push_result.returncode != 0:
    print(f"❌ Docker push failed")
    exit(1)

print(f"✅ Image pushed: {image_tag}")

# ============================================================================
# UPDATE LAMBDA
# ============================================================================

lambda_client = session.client('lambda', region_name='eu-west-3')
for lambda_name in lambda_names:
    try:
        print(f"🔄 Updating Lambda: {lambda_name}")
        response = lambda_client.update_function_code(
            FunctionName=lambda_name, 
            ImageUri=image_tag
        )
        print(f"✅ Updated Lambda: {lambda_name}")
        
        # Afficher quelques infos sur la Lambda
        print(f"   📋 Function ARN: {response.get('FunctionArn', 'N/A')}")
        print(f"   📋 Last Modified: {response.get('LastModified', 'N/A')}")
        
    except Exception as e:
        if 'ResourceNotFoundException' in str(e):
            print(f"🔄 Creating new Lambda: {lambda_name}")
            try:
                response = lambda_client.create_function(
                    FunctionName=lambda_name,
                    Role='arn:aws:iam::430054308525:role/Winpharma_Lambda',
                    Code={'ImageUri': image_tag},
                    PackageType='Image',
                    Timeout=300,  # 5 minutes pour les gros volumes
                    MemorySize=512,  # Plus de mémoire pour les traitements
                    Environment={
                        'Variables': {
                            'API_URL': 'YXBvdGhpY2Fs',
                            'API_PASSWORD': 'cGFzczE',
                            'PHARMACY_ID': '062044623',
                            'SERVER_URL': 'https://api.phardev.fr'
                        }
                    }
                )
                print(f"✅ Created Lambda: {lambda_name}")
            except Exception as create_error:
                print(f"❌ Error creating Lambda {lambda_name}: {create_error}")
                continue
        else:
            print(f"❌ Error updating Lambda {lambda_name}: {e}")
            continue

print(f"🎉 Deployment completed successfully!")
print(f"")
print(f"🧪 To test your Lambda:")
print(f"aws lambda invoke --function-name {lambda_names[0]} --payload '{{}}' response.json && cat response.json")