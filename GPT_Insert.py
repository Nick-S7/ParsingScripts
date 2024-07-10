from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime
import requests
import json
from datetime import datetime, timedelta
from snowflake.connector.pandas_tools import pd_writer
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import NVARCHAR
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
from airflow.models.xcom import XCom
from botocore.client import Config
import boto3
from airflow.models import Variable


awsAccessKey = Variable.get("aws_access_key_id")
awsSecretKey = Variable.get("aws_secret_access_key")
s3BucketData = Variable.get("s3BucketData")
s3BucketDag = Variable.get("s3BucketDag")
s3BucketHyper = Variable.get("s3BucketHyper")
vinylDomUrl = Variable.get("vinylDomUrl")
vinylApiKey = Variable.get("vinylApiKey")
env = Variable.get("env")

def create_dag():

    awsAccessKey = Variable.get("aws_access_key_id")
    awsSecretKey = Variable.get("aws_secret_access_key")
    s3BucketData = Variable.get("s3BucketData")
    s3BucketHyper = Variable.get("s3BucketHyper")
    vinylDomUrl = Variable.get("vinylDomUrl")
    vinylApiKey = Variable.get("vinylApiKey")

    configs3 = Config(connect_timeout=1000000, retries={'max_attempts': 10})

    s3 = boto3.resource(
        config=configs3,
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id=awsAccessKey,
        aws_secret_access_key=awsSecretKey
    )
          
#####=====+++++|\/|\/|\/|\/| GET CODE SNIPPET FROM S3 |\/|\/|\/|\/|+++++=====######

    codeBucket = "getourdata-codesnippets"
    for obj1 in s3.Bucket(codeBucket).objects.all():
        test = s3.Bucket(codeBucket).Object(obj1.key).get()
        codesnip = test['Body'].read().decode('utf-8')
        #print(tmpRead)


        
#####=====+++++|\/|\/|\/|\/| GET JSON FROM S3 |\/|\/|\/|\/|+++++=====######
    customerID = "JonSon"
    apiID = "Railz_123"
    jsonPath = customerID + "/" + apiID
    s3BucketData = "datafiles-dev"

    objs = list()
    for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=jsonPath):
        test = s3.Bucket(s3BucketData).Object(obj2.key).get()     
        if ".json" not in obj2.key:
            continue
        else:
            print(obj2.key)
            tmpRead = test['Body'].read()#.decode('utf-8')
            tmp2 = json.loads(tmpRead)
            #print(tmp2['data'])
            for ea in tmp2['data']:
                objs.append(ea)

    print(objs)
    df = pd.DataFrame(objs)
    print(df)
        #print(tmp2)
#        #print(tmpRead)
#        data = tmp2
##    print(type(str(data)), str(data))
#    jsonStr = "json_response = "+str(data)+"\n"
#    full = jsonStr+codesnip
#    #print(full)
#    exec(full, globals())

with DAG(

    dag_id = 'GPT_Insert',
    start_date = datetime(2022, 2, 12),
    catchup = False,
    schedule_interval='@hourly'

) as dag: 

    createDag = PythonOperator(
        task_id='createDag',
        python_callable=create_dag,
    )

createDag

