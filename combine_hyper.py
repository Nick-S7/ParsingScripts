from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
from botocore.client import Config
import pandas as pd
import pantab
from datetime import datetime
from airflow.models import Variable


def hyper_schedule():

    awsAccessKey = Variable.get("aws_access_key_id")
    awsSecretKey = Variable.get("aws_secret_access_key")

    s3BucketHyper = "dev-tableu-hyperfiles"
    
    configs3 = Config(connect_timeout=1000000, retries={'max_attempts': 10})

    s3 = boto3.resource(
        config=configs3,
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id=awsAccessKey,
        aws_secret_access_key=awsSecretKey
    )
    
    hasTotal = dict()
    #Hit bucket and get list of all obj in Bucket
    for obj1 in s3.Bucket(s3BucketHyper).objects.all():#filter(Prefix="/total"):
        #split Object key (path) based on "/"
        splitk = obj1.key.split("/")

        #get customer and api id from split path 
        customerID = splitk[0]
        ApiID = splitk[1]
        #combine to make dynamic customer based path
        custyPath = customerID + "/" + ApiID + "/"
        #add path (key) + bool (val) to dict to track which customer/api combos have a total folder or not.
        if splitk[2] == 'total':
            hasTotal[custyPath] = True
        else:
            hasTotal[custyPath] = False

    print(hasTotal)

    now = datetime.now()
    finalDict = dict()

    #for each customer path in the list
    for path, val in hasTotal.items():
        print(path, val)
        splitPath = path.split("/")
        print(splitPath)
        if val == False:
            print("flase workflow")
            customerID = splitPath[0]
            ApiID = splitPath[1]
            endpointsPath = customerID + '/' + ApiID + "/endpoints/"   
            print(endpointsPath)
            for obj2 in s3.Bucket(s3BucketHyper).objects.filter(Prefix=endpointsPath):
                print(obj2.key)
                print(s3.Bucket(s3BucketHyper).Object(obj2.key).get())
                test = s3.Bucket(s3BucketHyper).Object(obj2.key).get()
                print(test)
                #last = test['LastModified']
                #
                splitd = obj2.key.split("/")
                csv_string = test['Body']#.read()#.decode('utf-8')
                frame = pd.read_csv(csv_string)
                fkey = splitd[3].replace(".csv", '')
                dframe = frame.convert_dtypes(infer_objects=False)
                #print(dframe)
                finalDict[fkey] = dframe
            print(finalDict)
            
            pantab.frames_to_hyper(finalDict, ApiID+".hyper")
            #print(finalDict)
            totalPath = customerID + '/' + ApiID + '/total/'
            with open(ApiID+".hyper", 'r') as e:
               e.close()
               s3.Bucket(s3BucketHyper).upload_file(Filename=ApiID+".hyper", Key=totalPath+ApiID+".hyper")
            finalDict = {}
        else:
            print("true workflow")
            totalPath = path+"total/"
            print(totalPath)
            for obj3 in s3.Bucket(s3BucketHyper).objects.filter(Prefix=totalPath):
                    print(obj3)
                    print(obj3.key)
                    meta = s3.Bucket(s3BucketHyper).Object(obj3.key).get()
                    last = meta['LastModified']
                    #print(obj3.key)
                    difference = now - last.replace(tzinfo=None)
                    if difference.days >= 1:
                        customerID = splitPath[0]
                        ApiID = splitPath[1]
                        #print(obj1.key)
                        endpointsPath = customerID + '/' + ApiID + "/endpoints/"   
                        for obj2 in s3.Bucket(s3BucketHyper).objects.filter(Prefix=endpointsPath):
                            test = s3.Bucket(s3BucketHyper).Object(obj2.key).get()
                            #last = test['LastModified']
                            #
                            splitd = obj2.key.split("/")
                            csv_string = test['Body']#.read()#.decode('utf-8')
                            frame = pd.read_csv(csv_string)
                            fkey = splitd[3].replace(".csv", '')
                            dframe = frame.convert_dtypes(infer_objects=False)
                            #print(dframe)
                            finalDict[fkey] = dframe
                        print(finalDict)
                        pantab.frames_to_hyper(finalDict, ApiID+".hyper")
                        #print(finalDict)
                        totalPath = customerID + '/' + ApiID + '/total/'
                        with open(ApiID+".hyper", 'r') as e:
                           e.close()
                           s3.Bucket(s3BucketHyper).upload_file(Filename=ApiID+".hyper", Key=totalPath+ApiID+".hyper")
                        print(difference)
                        finalDict = {}
                    else:
                        print("Not a day old yet")

with DAG(
    dag_id = 'combine_hyper',
    start_date=datetime(2023, 2, 1),
    schedule_interval='@daily'
) as dag :

    Test_func = PythonOperator(
        task_id='Test_func',
        python_callable=hyper_schedule,
    )