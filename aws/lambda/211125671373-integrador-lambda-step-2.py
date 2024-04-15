import os
import pandas as pd
import boto3
import json
from datetime import datetime

def lambda_handler(event, context):

    # Obter mensagens do SQS 
    messages = event["Records"]
    print(f"Info: messages = {messages}")
    
    # Preparar clientes dos serviços usados
    s3 = boto3.client("s3")
    sqs = boto3.client("sqs")
    sns = boto3.client("sns")
    
    # Definir limites de qualidade
    minTemp = float(os.environ["MINTEMPERATURE"])
    maxTemp = float(os.environ["MAXTEMPERATURE"])
    topicArn = os.environ["SNSTOPICARN"]

    for message in messages:
        body = message.get('body')
        print(f"Info: body = {body}")
        
        # Ler json do S3 RAW
        fileName = f"{body}.json"
        s3Response = s3.get_object(Bucket=os.environ["RAWBUCKET"], Key=fileName)
        rawData = pd.read_json(s3Response.get("Body"))

        # Limpar dados
        trustedData = rawData.dropna()
        
        # Teste de qualidade
        if trustedData.isnull().values.any() or trustedData["analysed_sst"].min() < minTemp or trustedData["analysed_sst"].max() > maxTemp:
            alertMessage = f"Alerta de qualidade de dados para o dia {body}!\nO arquivo não foi processado em TRUSTED!\nEsta é uma mensagem automática. Por favor, não responda."
            snsResponse = sns.publish(TopicArn=topicArn, Subject="[Integrador] - ALERTA em TRUSTED", Message=alertMessage)
            print(alertMessage)
        else:
            # Salvar no S3 TRUSTED
            jsonFile = trustedData.to_json(orient="records")
            s3TrustedResponse = s3.put_object(Bucket=os.environ["TRUSTEDBUCKET"], Key=fileName, Body=jsonFile)
            sqsResponse = sqs.send_message(QueueUrl=os.environ["TRUSTEDSQS"], MessageBody=body)

    return {
        'statusCode': 200,
        'body': json.dumps('Step 2 finished!')
    }
