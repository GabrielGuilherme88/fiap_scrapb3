import boto3
from botocore.exceptions import NoCredentialsError
import yaml
import os

# Caminho do YAML de configuração geral
CONFIG_FILE = "config.yaml"
AWS_CREDENTIALS_FILE = "aws_credentials.yaml"

# Carregando configuração geral
with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)

BUCKET_NAME = config["s3"]["bucket_name"]
PREFIX = config["s3"]["prefix"]
HISTORICO_FILE = config["paths"]["historico_tickers"]

# Função para carregar credenciais AWS do YAML
def load_aws_credentials(yaml_file=AWS_CREDENTIALS_FILE):
    if os.path.exists(yaml_file):
        with open(yaml_file, "r") as f:
            creds = yaml.safe_load(f)
        return creds["aws"]
    else:
        return None

# Inicializa sessão S3 com credenciais automáticas
def get_s3_client():
    try:
        # Tenta criar sessão padrão (com credenciais do ambiente)
        session = boto3.Session()
        s3_client = session.client('s3')
        # Testa chamada simples para verificar credenciais
        s3_client.list_buckets()
        return s3_client
    except NoCredentialsError:
        # Se não houver credenciais, tenta ler do YAML
        creds = load_aws_credentials()
        if creds:
            session = boto3.Session(
                aws_access_key_id=creds["access_key_id"],
                aws_secret_access_key=creds["secret_access_key"],
                region_name=creds.get("region_name", "us-east-1")
            )
            print("ℹ️ Credenciais carregadas do YAML.")
            return session.client("s3")
        else:
            raise NoCredentialsError("❌ Nenhuma credencial encontrada e YAML não existe.")

# Função principal
def handle_s3(file_name=None, action="list", object_name=None, prefix=None):
    """
    Função utilitária para interagir com o S3 (upload, delete, list).
    """
    try:
        s3_client = get_s3_client()

        if action == 'upload':
            if file_name is None:
                raise ValueError("É necessário informar 'file_name' para upload.")

            if object_name is None:
                object_name = os.path.basename(file_name)
            if prefix:
                object_name = f"{prefix}/{object_name}"

            s3_client.upload_file(file_name, BUCKET_NAME, object_name)
            print(f"✅ Upload concluído: s3://{BUCKET_NAME}/{object_name}")

        elif action == 'delete':
            if object_name is None:
                raise ValueError("Para deletar, 'object_name' deve ser especificado.")

            s3_client.delete_object(Bucket=BUCKET_NAME, Key=object_name)
            print(f"🗑️ Objeto deletado: {object_name}")

        elif action == 'list':
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix if prefix else ''):
                for obj in page.get('Contents', []):
                    print(obj['Key'])

        else:
            print(f"⚠️ Ação '{action}' não reconhecida.")
            return False

    except NoCredentialsError as e:
        print(e)
        return False
    except Exception as e:
        print(f"❌ Erro na ação '{action}': {e}")
        return False

    return True