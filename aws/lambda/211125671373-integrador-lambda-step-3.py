import os
import pandas as pd
import boto3
import json
from datetime import datetime

def lambda_handler(event, context):

    # Obter mensagens do SQS TRUSTED
    messages = event["Records"]
    print(f"Info: messages = {messages}")
    
    s3 = boto3.client("s3")
    sqs = boto3.client("sqs")
    #firehose = boto3.client("firehose")

    for message in messages:
        body = message.get('body')
        print(f"Info: body = {body}")
        
        # Ler json do S3 TRUSTED
        fileName = f"{body}.json"
        s3Response = s3.get_object(Bucket=os.environ["TRUSTEDBUCKET"], Key=fileName)
        trustedData = pd.read_json(s3Response.get("Body"))

        # Consolidar dados
        kFactor = float(os.environ["KELVINFACTOR"])
        
        regId = int(os.environ["REGID"])
        dataIso = body[0:4] + "-" + body[4:6] + "-" + body[6:8]
        tempMin = trustedData["analysed_sst"].min() - kFactor
        tempMax = trustedData["analysed_sst"].max() - kFactor
        tempMed = trustedData["analysed_sst"].mean() - kFactor
        
        refinedData = {"regid": regId, "data": dataIso, "tempmin": tempMin, "tempmax": tempMax, "tempmed": tempMed}

        # Salvar no S3 REFINED
        dataStr = json.dumps(refinedData)
        s3RefinedResponse = s3.put_object(Bucket=os.environ["REFINEDBUCKET"], Key=fileName, Body=dataStr)
        
        # Enviar ao SQS para integração on premise
        sqsResponse = sqs.send_message(QueueUrl=os.environ["REFINEDSQS"], MessageBody=body)

        # Enviar ao Firehose
        #data64 = dataStr.encode("ascii")
        #firehose.put_record(DeliveryStreamName=os.environ["REFINEDFIREHOSE"], Record={"Data": data64})
            
    return {
        'statusCode': 200,
        'body': json.dumps('Step 3 finished!')
    }
