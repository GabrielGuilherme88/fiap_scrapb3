import json
import boto3

glue_client = boto3.client("glue")

def lambda_handler(event, context):
    """
    Função Lambda acionada por eventos do S3.
    Dispara dois Glue Jobs quando novo arquivo é colocado no bucket.
    """
    try:
        # Ler informações do arquivo no evento S3
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            print(f"Novo arquivo detectado: s3://{bucket}/{key}")

        # Iniciar primeiro Glue Job
        response1 = glue_client.start_job_run(JobName="bovespa-extract-notebook")
        print(f"Job bovespa-extract-notebook iniciado. RunId: {response1['JobRunId']}")

        # Iniciar segundo Glue Job
        response2 = glue_client.start_job_run(JobName="bovespa-extract-total-dia")
        print(f"Job bovespa-extract-total-dia iniciado. RunId: {response2['JobRunId']}")

        return {
            "statusCode": 200,
            "body": json.dumps("Jobs Glue disparados com sucesso!")
        }

    except Exception as e:
        print(f"Erro ao iniciar jobs Glue: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Erro: {str(e)}")
        }
