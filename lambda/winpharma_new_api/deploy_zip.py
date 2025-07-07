#!/usr/bin/env python3
"""
Déploiement Lambda avec ZIP (pas Docker)
Plus simple et ça marche toujours !
"""

import os
import shutil
import subprocess
import zipfile
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

lambda_name = 'winpharma_new_api_test'
zip_filename = 'lambda_deployment.zip'

# Configuration AWS
session = boto3.Session(region_name='eu-west-3')
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

def create_deployment_package():
    """Crée le package ZIP pour Lambda"""
    print(f"📦 Creating deployment package...")
    
    # Créer dossier temporaire
    temp_dir = Path('temp_lambda')
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir()
    
    try:
        # 1. Installer les dépendances
        print(f"📥 Installing dependencies...")
        subprocess.run([
            'pip', 'install', '-r', 'requirements.txt', '-t', str(temp_dir)
        ], check=True)
        
        # 2. Copier le code
        print(f"📄 Copying source code...")
        shutil.copy2('app.py', temp_dir / 'app.py')
        
        # 3. Créer le ZIP
        print(f"🗜️ Creating ZIP file...")
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = Path(root) / file
                    arcname = file_path.relative_to(temp_dir)
                    zipf.write(file_path, arcname)
        
        # 4. Nettoyer
        shutil.rmtree(temp_dir)
        
        zip_size = Path(zip_filename).stat().st_size / (1024 * 1024)
        print(f"✅ ZIP created: {zip_filename} ({zip_size:.1f} MB)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating package: {e}")
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        return False

def deploy_lambda():
    """Déploie la Lambda avec le ZIP"""
    print(f"🚀 Deploying Lambda function...")
    
    lambda_client = session.client('lambda', region_name='eu-west-3')
    
    try:
        # Lire le ZIP
        with open(zip_filename, 'rb') as f:
            zip_content = f.read()
        
        # Essayer de mettre à jour
        try:
            print(f"🔄 Updating existing Lambda: {lambda_name}")
            response = lambda_client.update_function_code(
                FunctionName=lambda_name,
                ZipFile=zip_content
            )
            print(f"✅ Updated Lambda: {lambda_name}")
            
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f"🔄 Creating new Lambda: {lambda_name}")
            response = lambda_client.create_function(
                FunctionName=lambda_name,
                Runtime='python3.9',
                Role='arn:aws:iam::430054308525:role/Winpharma_Lambda',
                Handler='app.handler',
                Code={'ZipFile': zip_content},
                Timeout=300,
                MemorySize=512,
                Environment={
                    'Variables': {
                        'API_URL': 'YXBvdGhpY2Fs',
                        'API_PASSWORD': 'cGFzczE',
                        'PHARMACY_ID': '692037567',
                        'SERVER_URL': 'https://api.phardev.fr'
                    }
                }
            )
            print(f"✅ Created Lambda: {lambda_name}")
        
        # Informations sur la fonction
        print(f"📋 Function ARN: {response.get('FunctionArn', 'N/A')}")
        print(f"📋 Runtime: {response.get('Runtime', 'N/A')}")
        print(f"📋 Handler: {response.get('Handler', 'N/A')}")
        print(f"📋 Last Modified: {response.get('LastModified', 'N/A')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error deploying Lambda: {e}")
        return False

def test_lambda():
    """Teste la Lambda déployée"""
    print(f"🧪 Testing Lambda function...")
    
    lambda_client = session.client('lambda', region_name='eu-west-3')
    
    try:
        response = lambda_client.invoke(
            FunctionName=lambda_name,
            Payload='{}'
        )
        
        status_code = response['StatusCode']
        print(f"📊 Status Code: {status_code}")
        
        if 'FunctionError' in response:
            print(f"❌ Function Error: {response['FunctionError']}")
            return False
        else:
            payload = response['Payload'].read().decode('utf-8')
            print(f"✅ Success! Response preview: {payload[:200]}...")
            return True
            
    except Exception as e:
        print(f"❌ Error testing Lambda: {e}")
        return False

def main():
    """Fonction principale"""
    print(f"🚀 LAMBDA ZIP DEPLOYMENT")
    print(f"=" * 50)
    
    try:
        # 1. Créer le package
        if not create_deployment_package():
            return False
        
        # 2. Déployer
        if not deploy_lambda():
            return False
        
        # 3. Tester
        if test_lambda():
            print(f"\n🎉 DEPLOYMENT SUCCESSFUL!")
            print(f"🧪 To test again:")
            print(f"aws lambda invoke --function-name {lambda_name} --payload '{{}}' response.json && cat response.json")
        else:
            print(f"\n⚠️ Deployment successful but test failed")
            print(f"🔍 Check logs: aws logs filter-log-events --log-group-name /aws/lambda/{lambda_name}")
        
        return True
        
    except KeyboardInterrupt:
        print(f"\n⏹️ Deployment interrupted by user")
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return False
    finally:
        # Nettoyer le ZIP
        if Path(zip_filename).exists():
            Path(zip_filename).unlink()
            print(f"🧹 Cleaned up {zip_filename}")

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)