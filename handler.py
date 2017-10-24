import json
import boto3
import hashlib
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.info("Handler avviato")


def hello(event, context):
    logger.info("Iniziamo..")
    logger.info(event)
    body = {
        "message": "Ciao sono una piccola funzione!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

def upload(evento, contenuto):
    logger.info(evento)
    
    datijson = json.loads(evento['body'])
    dati = datijson['data']
    
    hash_dati = hashlib.sha1(dati.encode('utf-8')).hexdigest()

    s3conn = boto3.resource('s3')
    s3conn.Bucket('bucketlxp').put_object(
        Key=hash_dati + '.json',
        Body=json.dumps(dati))

    body = {"message": "File caricato" }
    
    risposta = {
        "statusCode": 200,
        "body": "fatto"
    }
    return risposta


def scan(evento, contenuto):
    logger.info(evento)

    oggetto_chiave = evento['Records'][0]['s3']['object']['key']
    nomefiletmp = '/tmp/tmpfilein_' + oggetto_chiave

    logger.info("Abbiamo il file: " + oggetto_chiave)
    logger.info("nomefiletmp = " + nomefiletmp)

    s3conn = boto3.resource('s3')

    s3conn.Bucket('bucketlxp').download_file(oggetto_chiave, nomefiletmp)
    righe_problemi = []
    for riga in open(nomefiletmp, 'r'):
        if 'cattivo' in riga:
            righe_problemi.append(riga)

    logger.info("Righe con problemi = {}".format(righe_problemi))
    dynamodb = boto3.resource('dynamodb')
    tabella = dynamodb.Table('conproblemi')

    for riga in righe_problemi:
        tabella.put_item(Item={'riga': riga})

    response = {
        "statusCode": 200,
        "body": "Scan completato"
    }

    return response

def prendi_righe_cattive(evento,contenuto):
    dynamodb = boto3.resource('dynamodb')
    tabella = dynamodb.Table('conproblemi')
    oggetti = tabella.scan()
    return {
        "statusCode": 200,
        "body": json.dumps(oggetti)
    }
