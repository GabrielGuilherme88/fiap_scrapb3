from s3_utils import handle_s3
import os
import subprocess

if __name__ == "__main__":
    
    try:
        subprocess.run(["python3", "get_data_yf.py"], check=True)
        print("✅ Script get_data_yf.py executado com sucesso.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro ao executar get_data_yf.py: {e}")
        exit(1)  # interrompe o main se der erro

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_PARQUET = os.path.join(BASE_DIR, "historico_tickers.csv")

# Faz upload do Parquet para o S3
handle_s3(
    file_name=FILE_PARQUET,
    action="upload",
    prefix="raw"
)