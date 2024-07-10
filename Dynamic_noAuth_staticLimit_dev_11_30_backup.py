from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime
import requests
import json
from locale import normalize
from wsgiref import headers
import sys
import math
from datetime import datetime, timedelta
from snowflake.connector.pandas_tools import pd_writer
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import NVARCHAR
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
from airflow.models.xcom import XCom
from requests.auth import HTTPBasicAuth
from io import StringIO
from botocore.exceptions import ClientError
from botocore.client import Config
import os
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
import boto3
from airflow.models import Variable
import uuid
import re
import inspect
import pantab

awsAccessKey = Variable.get("aws_access_key_id")
awsSecretKey = Variable.get("aws_secret_access_key")
s3BucketData = Variable.get("s3BucketData")
s3BucketDag = Variable.get("s3BucketDag")
s3BucketHyper = Variable.get("s3BucketHyper")
vinylDomUrl = Variable.get("vinylDomUrl")
vinylApiKey = Variable.get("vinylApiKey")
env = Variable.get("env")

def create_dag(dag_id, schedule, default_args, config):

    awsAccessKey = Variable.get("aws_access_key_id")
    awsSecretKey = Variable.get("aws_secret_access_key")
    s3BucketData = Variable.get("s3BucketData")
    s3BucketHyper = Variable.get("s3BucketHyper")
    vinylDomUrl = Variable.get("vinylDomUrl")
    vinylApiKey = Variable.get("vinylApiKey")
    tagCustomerID = config['tagCustomerID']

    configs3 = Config(connect_timeout=1000000, retries={'max_attempts': 10})

    s3 = boto3.resource(
        config=configs3,
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id=awsAccessKey,
        aws_secret_access_key=awsSecretKey
    )

    def log_endpoint_no_data(ti, **kwargs):
        endpointID = ti.xcom_pull(key="endpointID", task_ids="TokenOffsetTest")

        url = vinylDomUrl+"/endpointlog"
        context = kwargs
        run_id = context['dag_run'].run_id


        payload = json.dumps({
          "endpointID": endpointID,
          "log": "No Data Returned From API",
          "runID": run_id,
          "EndpointLogTypeID": "525be0fe-374c-4332-9e1f-2162b601c190"
        })

        headers = {
          'X-API-KEY': vinylApiKey,
          'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        print(response.text)

    def log_count_after_write(endpointID, recCount, table_name, schema, db):

        url = vinylDomUrl+"/endpointlog"

        log = str(recCount) + " record(s) written to " + db +"_"+ schema +"_"+ table_name

        payload = json.dumps({
            #probably client/UserID
          "endpointID": endpointID,
          "records": recCount,
          "log": log,
          "EndpointLogTypeID": "2242e16b-85fd-4a01-9bec-b5d2d2d2bc91"
        })

        headers = {
          'X-API-KEY': vinylApiKey,
          'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        print(response.text)

    def tokenoffsettest(config: dict, ti):
        for k, v in config.items():
            param = {}
            if type(v) is str:
                ti.xcom_push(key=k, value=v.replace(" ", "%20"))
            elif type(v) is int:
                ti.xcom_push(key=k, value=v)
            elif type(v) is dict:
                for kk, vv in v.items():
                    param[kk] = vv.replace(" ", "%20")
                    ti.xcom_push(key=k, value=param)
            elif type(v) is list:
                for d in v:                   
                    param[d['key']] = d['value'].replace(" ", "%20")
                    print(param)
                ti.xcom_push(key=k, value=param)
                #print(v)
        return "values psuhed to xcom"    
    
    def fetch_data(ti,**kwargs) -> None:
        #headers = {'Authorization' : 'Bearer ' + ti.xcom_pull(key="token",task_ids="Fetch_Token")}
        rqt = ti.xcom_pull(key="requesttype", task_ids="TokenOffsetTest")
        configHeaders = ti.xcom_pull(key="headers", task_ids="TokenOffsetTest")

        if configHeaders:
            headers = configHeaders
        else:
            headers = None

        dataUrl = ti.xcom_pull(key="dataUrl", task_ids="TokenOffsetTest")
        endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        params = ti.xcom_pull(key="param", task_ids="TokenOffsetTest")
        payload = ti.xcom_pull(key="payload", task_ids="TokenOffsetTest")
        recLim = ti.xcom_pull(key="inMemoryRecordLimit", task_ids="TokenOffsetTest")
        #make check for params returning as None instead of empty dict
        print(params)
        print(headers)

        if params:
            paramG2=str()
            #print(params)
            #print(url)
            ci = 0
            for k, v in params.items():
                if ci == 0:
                    paramG2 = str(k)+"="+str(v)
                    ci+=1
                else:
                    paramG2 =  paramG2 +"&"+ str(k)+"="+str(v) 
            url2 = dataUrl+ "/"+ endpoint + "?"+paramG2
        else:
            url2 = dataUrl+ "/"+ endpoint
        
        now = datetime.now()
        database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
        schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")
        dt = now.strftime("%d-%m-%Y %H:%M:%S")
        customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
        staticLimit = ti.xcom_pull(key="staticLimit", task_ids="TokenOffsetTest")

        if staticLimit:
            totalIterations = 1
            limit = staticLimit

        offsetType = ti.xcom_pull(key="offsetType", task_ids="TokenOffsetTest")
        limitType = ti.xcom_pull(key="staticLimitType", task_ids="TokenOffsetTest")

        hitCount = 0
        if int(limit) > 10000:
            newRecLim = recLim / 500 
        else:
            newRecLim = recLim / 20 
        newOffset = 0
        currentIterations = 0
        staging = list()
        #need to have it conitinue until response is empty without infinite loop.
        #Possibly add +1 to total iterations every call so theres no stopping, then if check res for data/empty list if empty list break/continue else keep looping 
        
        while currentIterations < totalIterations:
            print(url2)   
            if params:
                newUrl = url2 + "&"+ offsetType +"=" + str(newOffset)+ "&"+ limitType +"=" + str(limit)
            else:
                newUrl = url2 + "?"+ offsetType +"=" + str(newOffset)+ "&"+ limitType +"=" + str(limit)

            if payload:
                response = requests.request(rqt, newUrl, headers=headers, json=payload).json()
            else:
                print(rqt, newUrl, headers)
                response = requests.request(rqt, newUrl, headers=headers).json()
            print(newUrl, response)
            
            if response:
                currentIterations+=1
                print("limit ---->", limit)
                newOffset = currentIterations * int(limit)
                totalIterations +=1
                print("current---->", currentIterations, "total----->", totalIterations)
            else:
                currentIterations+=1
                    
            local_path=os.path.join(os.getcwd(), 'TEST_API_DATA.json')
            if customEndpointName:
                endpoint = customEndpointName
            else:
                endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
            print(database, schema, endpoint, str(dt))
            ApiResFilePath = tagCustomerID +"/"+ schema +"/" + endpoint + "/" + str(dt) +"/od_response/"
            #print(json.dumps(response))
            for e in response:                    
                staging.append(e)
            hitCount += 1
            print(hitCount/int(newRecLim))
            if (hitCount/int(newRecLim)).is_integer() is True:
                with open(local_path, 'w') as f:
                    f.write(json.dumps(staging))
                    print("staging1 -====>", json.dumps(staging))
                    print('file Written')
                    f.close()
                    s3.Bucket(s3BucketData).upload_file(Filename="TEST_API_DATA.json", Key= ApiResFilePath+ "data_"+str(newOffset)+"_.json")   
                staging=[]
        if (hitCount/int(newRecLim)).is_integer() is False:
            with open(local_path, 'w') as f:
                f.write(json.dumps(staging))
                print("staging2 -====>", json.dumps(staging))
                print('file Written')
                f.close()
                s3.Bucket(s3BucketData).upload_file(Filename="TEST_API_DATA.json", Key= ApiResFilePath+ "data_"+str(newOffset)+"_.json")                 
        ti.xcom_push(key='s3FP', value=ApiResFilePath)
        ti.xcom_push(key='dt', value=dt)

    def keyrelationships(ti):
            path = ti.xcom_pull(key="s3FP", task_ids="Fetch_Data")
            dt = ti.xcom_pull(key="dt", task_ids="Fetch_Data")
            database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
            endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
            customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
            schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")
            recLim = ti.xcom_pull(key="inMemoryRecordLimit", task_ids="TokenOffsetTest")
            staticLimit = ti.xcom_pull(key="staticLimit", task_ids="TokenOffsetTest")
            limit = staticLimit
            #print(endpoint, customEndpointName, schema)
            #print(customEndpointName)

            if customEndpointName:
                endpoint = customEndpointName
            else:
                endpoint = endpoint

            endpoint = endpoint.replace("/", "_")

            #print(endpoint)

            local_path=os.path.join(os.getcwd(), 'keyrelationships_DATA.json')
            newPath = tagCustomerID +"/"+ schema +"/" + endpoint + "/" + str(dt) +"/od_keys/"
            staging = list()
            count = 0
            for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=path):         
            
                test = s3.Bucket(s3BucketData).Object(obj2.key).get()
                #print(test)
                tmpRead = test['Body'].read().decode('utf-8')
                print(tmpRead)
                resx = json.loads(tmpRead)
                print(resx)
                for every in resx:
                    print(count)
                    count +=1
                    #print(resx)
                    primaryKeyVal = str(uuid.uuid4())
                    #print(primaryKeyVal)
                    x = {}
                    obj23 = {endpoint+ "_GID": primaryKeyVal, "TablePrefix": endpoint}
                    x = {**obj23, **every}
                    #print(json.dumps(x))
                    #just appending new primary key value (endpoint) with count as value to first level response obj's
                    #print(x)
                    for y in x: #level 1 keys ex. 'status', 'name', 'naics', etc.
                        if type(x[y]) is dict:
                            for z in x[y]: # {{}}
                                if type(x[y][z]) is list:
                                    listed = [] 
                                    for a in x[y][z]: #{{[]}}
                                        #print(a)
                                        if type(a) is dict:
                                            continue

                                        else:
                                            listdex1 = str(uuid.uuid4())
                                            end={}
                                            end[endpoint+ "_GID"] = primaryKeyVal
                                            end[z+"_GID"] = listdex1

                                            #print(end)

                                            end[z] = a

                                            listed.append(end) 
                                        #print(listed)

                                        x[y][z] = listed                                 
                        elif type(x[y]) is list: #{[]}
                            listed2 = []
                            listed3 = []
                            for z in x[y]:
                                #print(z)
                                if type(z) is dict:# level 1 list with dict
                                    #print("z that are dict", z)
                                    listdex2 = str(uuid.uuid4())
                                    #end2 = {}
                                    p = { endpoint+"_"+y+"_GID": listdex2, "TablePrefix": endpoint+"_"+y, endpoint+ "_GID": primaryKeyVal}
                                    z = {**p, **z}
                                    #print(z)
                                    listed3.append(z)
                                    #print(listed3)
                                    #continue #for now
                                    x[y] = listed3
                                    for a in z:
                                        obj4={}
                                        listed4 = []
                                        if type(z[a]) is list:
                                            for b in z[a]:
                                                if type(b) is dict:
                                                    listdex4 = str(uuid.uuid4())
                                                    o ={endpoint+"_"+y+"_"+a+"_GID": listdex4, "TablePrefix": endpoint+"_"+y+"_"+a, endpoint+"_"+y+"_GID": listdex2}
                                                    b = {**o, **b}
                                                    listed4.append(b)

                                                    for c in b:
                                                        #print(c, b[c])
                                                        if type(b[c]) is list:
                                                            #print(b[c])
                                                            listed5 = []
                                                            for d in b[c]:
                                                                if type(d) is dict:
                                                                    listdex5 = str(uuid.uuid4())
                                                                    o2 ={endpoint+"_"+y+"_"+a+"_"+c+"_GID": listdex5, "TablePrefix": endpoint+"_"+y+"_"+a+"_"+c, endpoint+"_"+y+"_"+a+"_GID": listdex4}
                                                                    d = {**o2, **d}
                                                                    listed5.append(d)
                                                                else:
                                                                    continue
                                                            b[c] = listed5
                                                            #print(b[c])
                                                        else:
                                                            continue
                                                else:
                                                    continue
                                            #print(listed4)
                                            z[a] = listed4
                                            #print(a, z[a]) 
                                        elif type(z[a]) is dict: #This is body { paging{}, paystatements{}}p
                                            #print(z[a])
                                            listdex4 = str(uuid.uuid4())
                                            for b in z[a]:
                                                #print(z[a][b]) 
                                                if type(z[a][b]) is dict: #this is paging {count, offset}
                                                    continue
                                                    #for c in b:
                                                    #    if type(b[c]) is list:
                                                    #        listdex5 = 0
                                                    #        listed5 = []
                                                    #        for d in b[c]:
                                                    #            if type(d) is dict:
                                                    #                listdex5 +=1
                                                    #                o2 ={"TablePrefix": endpoint+"_"+y+"_"+a+"_"+c, endpoint+"_"+y+"_"+a+"_ID": listdex4, endpoint+"_"+y+"_"+a+"_"+c+"_ID": listdex5}
                                                    #                d = {**o2, **d}
                                                    #                listed5.append(d)
                                                    #            else:
                                                    #                continue
                                                    #        b[c] = listed5
                                                    #    else:
                                                    #        continue
                                                elif type(z[a][b]) is list:
                                                    listed6 = []
                                                    for c in z[a][b]:
                                                        #print(type(c), c )
                                                        if type(c) is str:
                                                            listed6.append(c)
                                                        else:
                                                            listdex6 = str(uuid.uuid4())
                                                            o2 ={endpoint+"_"+y+"_"+a+"_"+b+"_GID": listdex6, "TablePrefix": endpoint+"_"+y+"_"+a+"_"+b, endpoint+"_"+y+"_GID": listdex2}
                                                            c = {**o2, **c}
                                                            listed6.append(c)
                                                    z[a][b] = listed6
                                                    #print(z[a][b])
                                                else:
                                                    continue
                                            #print(z[a])
                                            #listed4.append(z[a])
                                            obj4 = z[a]

                                        else:
                                              continue  
                                          
                                        #z[a] = listed4 
                                        #z[a] = obj4 <--- commented out 12/5/22 to make work with railz may need to switch back for others
                                elif type(z) is list:
                                    continue
                                else:
                                    listdex3 = str(uuid.uuid4())
                                    end2={}
                                    end2[endpoint+ "_GID"] = primaryKeyVal
                                    end2[y+"_GID"] = listdex3

                                    end2[y] =  z

                                    listed2.append(end2)

                                if type(z) is not dict:
                                    x[y] = listed2
                    staging.append(x)
                    if int(limit) > 10000:
                        newRecLim = recLim * 4 
                    else:
                        newRecLim = recLim / 20 
                    if (count/int(newRecLim)).is_integer() is True:
                        with open(local_path, 'w') as f:
                            f.write(json.dumps(staging))
                            f.close()
                            s3.Bucket(s3BucketData).upload_file(Filename="keyrelationships_DATA.json", Key= newPath+str(count)+".json")   
                            print("file written to s3")
                        staging=[]
                        print(json.dumps(x))
            if (count/int(newRecLim)).is_integer() is False:
                with open(local_path, 'w') as f:
                    f.write(json.dumps(staging))
                    f.close()
                    s3.Bucket(s3BucketData).upload_file(Filename="keyrelationships_DATA.json", Key= newPath+str(count)+".json")   
                    print("file written to s3")

    def objects_1_2(ti):

        recLim = ti.xcom_pull(key="inMemoryRecordLimit", task_ids="TokenOffsetTest")
        dt = ti.xcom_pull(key="dt", task_ids="Fetch_Data")
        database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
        endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
        schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")

        if customEndpointName:
            endpoint = customEndpointName
        else:
            endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")

        endpoint = endpoint.replace("/", "_")

        local_path=os.path.join(os.getcwd(), 'lvl1dictDATA.json')
        newPath = tagCustomerID +"/"+ schema +"/" + endpoint + "/" + str(dt) +"/od_objects/1-2/"
        path = tagCustomerID +"/"+ schema +"/" + endpoint + "/" + str(dt) +"/od_keys/"

        count = 0
        listy = []
        listtos3 = {}

        for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=path):
            test = s3.Bucket(s3BucketData).Object(obj2.key).get()
            tmpRead = test['Body'].read().decode('utf-8')
            all = json.loads(tmpRead)
            for d in all:
                count +=1
                print(type(d), d)
                #d = json.loads(d)
                out = {}
                out2 = {}
                if type(d) is dict:
                    for y in d:
                        #print(y)
                        if type(d[y]) is list:
                            continue
                        else:
                            out[y] = d[y]
                #print(out) #at this point we have obj with str's on 1st lvl and any dicts 
                for x in out:           
                    #print(x, out[x], type(out[x]))
                    if type(out[x]) is dict:
                        #print(x)
                        for a in out[x]:
                            if type(out[x][a]) is dict:
                                for z in out[x][a]:
                                    if type(out[x][a][z]) is dict or type(out[x][a][z]) is list:
                                        continue
                                    else:
                                        newKey = a + "_" + z
                                        out2[newKey] = out[x][a][z]
                            elif type(out[x][a]) is list:
                                continue
                            else:
                                newKey = x + "_" + a
                                out2[newKey] = out[x][a]
                    elif type(out[x]) is list:
                        continue
                    else:
                        out2[x] = out[x]
                        #print(toList)
                #print(out2)
                listy.append(out2)
                if (count/int(recLim)).is_integer() is True:
                    listtos3[endpoint] = listy
                    with open(local_path, 'w') as f:
                        f.write(json.dumps(listtos3))
                        print('file Written') ##########FILE KEEP RESETTING At 10 so it gets over written in s3, need different count for files.
                        f.close()
                        s3.Bucket(s3BucketData).upload_file(Filename="lvl1dictDATA.json", Key= newPath+str(count)+".json")
                    listtos3[endpoint] = []
                    listy=[]
        if (count/int(recLim)).is_integer() is False:
            listtos3[endpoint] = listy
            with open(local_path, 'w') as f:
                f.write(json.dumps(listtos3))
                print('file Written') ##########FILE KEEP RESETTING At 10 so it gets over written in s3, need different count for files.
                f.close()
                s3.Bucket(s3BucketData).upload_file(Filename="lvl1dictDATA.json", Key= newPath+str(count)+".json")
            listtos3[endpoint] = []
        #print(listtos3)

    def objects_1_2_snowFlake(ti):
        dt = ti.xcom_pull(key="dt", task_ids="Fetch_Data")
        database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
        endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
        schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")
        formatRecordLimit = ti.xcom_pull(key="formatRecordLimit", task_ids="TokenOffsetTest")
        endpointID = ti.xcom_pull(key="endpointID", task_ids="TokenOffsetTest")

        destinationUsername = ti.xcom_pull(key="destinationusername", task_ids="TokenOffsetTest")
        destinationPass = ti.xcom_pull(key="destinationpassword", task_ids="TokenOffsetTest")
        warehouse = ti.xcom_pull(key="warehouse", task_ids="TokenOffsetTest")
        role = ti.xcom_pull(key="role", task_ids="TokenOffsetTest")
        locator = ti.xcom_pull(key="locator", task_ids="TokenOffsetTest")
        region = ti.xcom_pull(key="region", task_ids="TokenOffsetTest")
        destinationType = ti.xcom_pull(key="destinationType", task_ids="TokenOffsetTest")

        print(schema)
        if customEndpointName:
            endpoint = customEndpointName
        else:
            endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")

        endpoint = endpoint.replace("/", "_")
        newPath = tagCustomerID +"/"+ schema +"/" + endpoint +"/"+ str(dt) + "/od_objects/1-2/"
        print(database.upper(), schema.upper())
        #Create connection to Snowflake using your account and user
        destinationUrl = locator + "." + region
        account_identifier = destinationUrl
        user = destinationUsername
        password = destinationPass
        warehouse = warehouse
        database_name = database.upper()
        schema_name = schema
        role = role
        conn_string = f"snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse}&role={role}"
        engine = create_engine(conn_string)
        table_name = endpoint.replace("/", "_").upper()   
        table_name = table_name.replace(".", "_") 
        #table_name = 'test'
        #df = pd.DataFrame()


        if destinationType == 'Tableau':
            finaldf= pd.DataFrame()
            df = pd.DataFrame()
            #print(newPath)
            listy=[]
            for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=newPath):
                test = s3.Bucket(s3BucketData).Object(obj2.key).get()
                tmpRead = test['Body'].read().decode('utf-8')
                tmp2 = json.loads(tmpRead)
                #print(x)
                for x in tmp2:
                    if type(tmp2[x]) is list:
                        for y in tmp2[x]:
                            listy.append(y)   
                    #listy2= listy2 + listy
                    #print(listy)
                    tempdf = pd.json_normalize(listy)
                    finaldf = pd.concat([df, tempdf])
                    #print(finaldf)
                    #finaldf = finaldf.rename(lambda x: x.upper(), axis=1)
            finaldf.columns = map(str.upper, finaldf.columns)

            for z in finaldf.columns:
                finaldf = finaldf.rename(columns={z: z.replace(".", "_")})
                finaldf = finaldf.rename(columns={z: z.replace("/", "_")})    
                #if "TABLEPREFIX" in z:
                #    finaldf.pop(z)
                #print(z)

            count_row = finaldf.shape[0]  # Gives number of rows

            if count_row < formatRecordLimit:
                max = count_row
            else:
                max = formatRecordLimit

            pattern = re.compile("^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+|.{0})(?:Z|[+-][01]\d:[0-5]\d)$")
            count = 0
            #print(test)
            dateCols=[]
            while count < max:
                test = finaldf.iloc[count,:]
                for i, v in test.items():
                    if type(v) is str:
                        check = bool(pattern.match(v))
                        if check is True:
                            dateCols.append(i)
                count+=1
                print(count)
            noDupes = list(set(dateCols))
            #print(noDupes)
            for col in dateCols:
                #finaldf[col] = pd.to_datetime(finaldf)
                finaldf[col] = pd.to_datetime(finaldf[col])
            #print(finaldf)

        
            tabPath = tagCustomerID + "/" + tagAPIID

            object = s3.Object(
                bucket_name=s3BucketHyper,
                key=tabPath+'/endpoints/'+table_name+'.csv'
            )

            print(finaldf)
            
            csv_buffer = StringIO()
            finaldf.to_csv(csv_buffer, index=False)
            object.put(Body=csv_buffer.getvalue())
            log_count_after_write(endpointID, count_row, table_name, schema, database)
        else:

            finaldf= pd.DataFrame()
            df = pd.DataFrame()
            #print(newPath)
            listy=[]
            for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=newPath):
                test = s3.Bucket(s3BucketData).Object(obj2.key).get()
                tmpRead = test['Body'].read().decode('utf-8')
                tmp2 = json.loads(tmpRead)
                #print(x)
                for x in tmp2:
                    if type(tmp2[x]) is list:
                        for y in tmp2[x]:
                            listy.append(y)   
                    #listy2= listy2 + listy
                    #print(listy)
                    tempdf = pd.json_normalize(listy)
                    finaldf = pd.concat([df, tempdf])
                    #print(finaldf)
                    #finaldf = finaldf.rename(lambda x: x.upper(), axis=1)
                count_row = finaldf.shape[0]  # Gives number of rows
                print('count row ----->', count_row)

#            finaldf.columns = map(str.upper, finaldf.columns)
#
#            for z in finaldf.columns:
#                finaldf = finaldf.rename(columns={z: z.replace(".", "_")})
#                finaldf = finaldf.rename(columns={z: z.replace("/", "_")})    
#                #if "TABLEPREFIX" in z:
#                #    finaldf.pop(z)
#                #print(z)
#
#            count_row = finaldf.shape[0]  # Gives number of rows
#
#            if count_row < formatRecordLimit:
#                max = count_row
#            else:
#                max = formatRecordLimit
#
#            pattern = re.compile("^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+|.{0})(?:Z|[+-][01]\d:[0-5]\d)$")
#            count = 0
#            #print(test)
#            dateCols=[]
#            while count < max:
#                test = finaldf.iloc[count,:]
#                for i, v in test.items():
#                    if type(v) is str:
#                        check = bool(pattern.match(v))
#                        if check is True:
#                            dateCols.append(i)
#                count+=1
#                print(count)
#            noDupes = list(set(dateCols))
#            #print(noDupes)
#            for col in dateCols:
#                #finaldf[col] = pd.to_datetime(finaldf)
#                finaldf[col] = pd.to_datetime(finaldf[col])
#
#
#
#            #print(finaldf)
#            print(conn_string)
#            #What to do if the table exists? replace, append, or fail?
#            if_exists = 'replace'
#            #Write the data to Snowflake, using pd_writer to speed up loading
#            with engine.connect().execution_options(autocommit=True) as con:
#                try:
#                    sql1 = (f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
#                    sqlRes = pd.read_sql(sql1, engine)
#                except:
#                    print("error creating schema / doesnt exist")
#
#                try:
#                    sql2 = (f'DROP TABLE {table_name};')
#                    sqlRes = pd.read_sql(sql2, engine)
#                except:
#                    print("error dropping table / doesnt exist")
#                
#                try:
#                    finaldf.to_sql(
#                        name= table_name,
#                        con=engine, 
#                        if_exists=if_exists,
#                        method=pd_writer,
#                        index=False
#                    )
#                    print('Dictionary written to Snowflake!')
#                    sql = (f'SELECT * FROM {table_name};')
#                    df_check = pd.read_sql(sql, engine)
#                    print(df_check.head(10))
#                    log_count_after_write(endpointID, count_row, table_name, schema, database)
#                except:
#                    print('Issue writing to Snowflake')

    def arrays_1(ti):   

        dt = ti.xcom_pull(key="dt", task_ids="Fetch_Data")
        database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
        endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
        schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")
        recLim = ti.xcom_pull(key="inMemoryRecordLimit", task_ids="TokenOffsetTest")
        newRecLim = recLim / 20 

        if customEndpointName:
            endpoint = customEndpointName
        else:
            endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")

        endpoint = endpoint.replace("/", "_")

        local_path=os.path.join(os.getcwd(), 'l1_list_w_dict_DATA.json')
        newPath = tagCustomerID +"/"+ schema +"/" + endpoint +"/"+ str(dt) +"/"

        path = tagCustomerID +"/"+ schema +"/" + endpoint + "/" + str(dt) +"/od_keys/"
        count = 0
        #print(path, dt, database, endpoint, schema, local_path, newPath)
        staging = list()
        countObj={}
        listCount = []
        listtos3 = {}
        for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=path):
            test = s3.Bucket(s3BucketData).Object(obj2.key).get()
            tmpRead = test['Body'].read().decode('utf-8')
            all = json.loads(tmpRead)
            for x in all:
                count +=1
                out = {}
                listOut = []
                print("x ------>", x)
                if type(x) is dict:
                    for y in x:
                        if type(x[y]) is list:
                            print(y)
                            for z in x[y]:
                                if type(z) is dict:
                                    out[y] = x[y] 
                    listOut.append(out)                
                print("out before ------>", json.dumps(out))
                for y in out:
                    out1 = {}
                    out2 = {}
                    listy=[]
                    if type(out[y]) is list:
                        for z in out[y]:
                            if type(z) is dict:
                                out3 = {}
                                for a in z:
                                    if type(z[a]) is dict:
                                        for b in z[a]:
                                            if type(z[a][b]) is list: 
                                                continue
                                            elif type(z[a][b]) is dict:
                                                continue
                                            else:
                                                newkey = a + '_' + b
                                                out3[newkey] = z[a][b]
                                    elif type(z[a]) is list:
                                        continue
                                    else:
                                        out3[a] = z[a]
                                    #print(json.dumps(out3))
                            elif type(z) is list:
                                continue
                            else:
                                continue
                            if countObj:
                                #print(countObj)
                                for k in list(countObj):
                                    newKey = y 
                                    #print(k, c)
                                    if newKey == k and newKey in listCount: 
                                        #print("if k = a")
                                        countObj[newKey] += 1
                                        listtos3[newKey].append(out3)  
                                        print(countObj, listtos3[newKey])
                                        if (countObj[newKey]/int(recLim)).is_integer() is True:
                                            #print(listtos3[newKey],"divisible by 5")
                                            tmpDict={}
                                            Tpre = endpoint + "_" +k.replace("/", "_")
                                            tmpDict["TablePrefix"] = Tpre
                                            tmpDict[newKey] = listtos3[newKey]
                                            #print("at 50 writing-->", tmpDict)
                                            file = str(uuid.uuid4())
                                            with open(local_path, 'w') as f:
                                                f.write(json.dumps(tmpDict))
                                                print('file Written') ##########FILE KEEP RESETTING At 10 so it gets over written in s3, need different count for files.
                                                print(newPath+y+"/"+file)
                                                f.close()
                                                s3.Bucket(s3BucketData).upload_file(Filename="l1_list_w_dict_DATA.json", Key= newPath+y+"/"+file+".json")
                                            #print(listtos3[newKey])
                                            listtos3[newKey] = []
                                    elif newKey != k and newKey not in listCount:
                                        #print("if k DOES NOT = a")
                                        newKey = y 
                                        listCount.append(newKey)
                                        countObj[newKey] = 1
                                        listtos3[newKey] = [out3]
                                        #print(countObj)
                                        #print(countObj, listtos3[newKey])
                                    else:
                                        print("Adds to nothing")
                            else:
                                #print("countOBJ empty")
                                newKey = y 
                                listCount.append(newKey)
                                countObj[newKey] = 1
                                listtos3[newKey] = [out3]
                                #print(countObj, listtos3[newKey])
        for k,v in listtos3.items():
            finalDict={}
            Tpre = endpoint + "_" +k
            finalDict["TablePrefix"] = Tpre
            finalDict[k] = v
            if (countObj[k]/int(recLim)).is_integer() is False:
                #k = k.replace("_", "/")
                file = str(uuid.uuid4())
                print("writing extra to s3", newPath+ k+file)
                with open(local_path, 'w') as f:
                    f.write(json.dumps(finalDict))
                    print('remainder of files Written')
                    f.close()
                    s3.Bucket(s3BucketData).upload_file(Filename="l1_list_w_dict_DATA.json", Key= newPath+k+"/"+file+".json")
            else:
                print("nothing to write to s3")
                continue

    def arrays_1_snowFlake(ti):
        # try:
             #response = ti.xcom_pull(key='return_value', task_ids='Dict_Flat_Json')
             #path = context["dag_run"].conf['s3FP']

        dt = ti.xcom_pull(key="dt", task_ids="Fetch_Data")
        database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
        endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
        schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")
        formatRecordLimit = ti.xcom_pull(key="formatRecordLimit", task_ids="TokenOffsetTest")
        endpointID = ti.xcom_pull(key="endpointID", task_ids="TokenOffsetTest")

        destinationUsername = ti.xcom_pull(key="destinationusername", task_ids="TokenOffsetTest")
        destinationPass = ti.xcom_pull(key="destinationpassword", task_ids="TokenOffsetTest")
        warehouse = ti.xcom_pull(key="warehouse", task_ids="TokenOffsetTest")
        role = ti.xcom_pull(key="role", task_ids="TokenOffsetTest")
        locator = ti.xcom_pull(key="locator", task_ids="TokenOffsetTest")
        region = ti.xcom_pull(key="region", task_ids="TokenOffsetTest")
        destinationType = ti.xcom_pull(key="destinationType", task_ids="TokenOffsetTest")

        if customEndpointName:
            endpoint = customEndpointName
        else:
            endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        
        endpoint = endpoint.replace("/", "_")

        newPath = tagCustomerID +"/"+ schema +"/" + endpoint +"/"+ str(dt)+"/"

        #Create connection to Snowflake using your account and user
        destinationUrl = locator + "." + region
        account_identifier = destinationUrl
        user = destinationUsername
        password = destinationPass
        warehouse = warehouse
        database_name = database.upper()
        schema_name = schema
        role = role
        conn_string = f"snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse}&role={role}"
        engine = create_engine(conn_string)
        #Create your DataFrame
        #listy2 = []

        #print(conn_string)
        fList = []
        for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=newPath):
            test = s3.Bucket(s3BucketData).Object(obj2.key).get()
            okey = obj2.key
            #print(okey)
            splitd = okey.split("/")
            fList.append(splitd[4])
            #print(fList)    
            fList = list(set(fList))
            for x in fList:
                if "od_response" in x:
                    fList.remove(x)
                elif "od_objects" in x:
                    fList.remove(x)
                elif "od_keys" in x:
                    fList.remove(x)
                elif ".json" in x:
                    fList.remove(x)
        print(fList)
        for x in fList:
            print("x---->", x)
            newPath = tagCustomerID +"/"+ schema +"/" + endpoint +"/" + str(dt) +"/" + x
            finaldf = pd.DataFrame()
            listy = []
            for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=newPath+"/", Delimiter='/'):
                #print("newPath --->", newPath)
                test = s3.Bucket(s3BucketData).Object(obj2.key).get()
                okey = obj2.key
                #print(okey)
                tmpRead = test['Body'].read().decode('utf-8')
                all = json.loads(tmpRead)
                print(all)
                print(all["TablePrefix"])
                print(tmpRead)

                
                df = pd.DataFrame()
                newtablename = all["TablePrefix"]
                finaltablename = newtablename.upper()
                finaltablename = finaltablename.replace(".", "_")
                finaltablename = finaltablename.replace("/", "_")
                for x in all:
                    if type(all[x]) is list:
                        for y in all[x]:
                            listy.append(y)   
                    #listy2= listy2 + listy
                    #print(listy)
                    tempdf = pd.json_normalize(listy)
                    finaldf = pd.concat([df, tempdf])
                    #print(finaldf)
                    #finaldf = finaldf.rename(lambda x: x.upper(), axis=1)
            finaldf.columns = map(str.upper, finaldf.columns)

            for z in finaldf.columns:
                finaldf = finaldf.rename(columns={z: z.replace(".", "_")})
                finaldf = finaldf.rename(columns={z: z.replace("/", "_")})
                if z == "TABLEPREFIX":
                    finaldf.pop(z)
            #print(finaldf)

            count_row = finaldf.shape[0]  # Gives number of rows

            if count_row < formatRecordLimit:
                max = count_row
            else:
                max = formatRecordLimit

            pattern = re.compile("^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+|.{0})(?:Z|[+-][01]\d:[0-5]\d)$")
            count = 0
            #print(test)
            dateCols=[]
            while count < max:
                test = finaldf.iloc[count,:]
                for i, v in test.items():
                    if type(v) is str:
                        check = bool(pattern.match(v))
                        if check is True:
                            dateCols.append(i)
                count+=1
                #print(count)
            noDupes = list(set(dateCols))
            #print(noDupes)
            for col in dateCols:
                #finaldf[col] = pd.to_datetime(finaldf)
                finaldf[col] = pd.to_datetime(finaldf[col])

            if destinationType == 'Tableau':
                tabPath = tagCustomerID + "/" + tagAPIID

                object = s3.Object(
                    bucket_name=s3BucketHyper,
                    key=tabPath+'/endpoints/'+finaltablename+'.csv'
                )

                print(finaldf.dtypes)

                csv_buffer = StringIO()
                finaldf.to_csv(csv_buffer, index=False)
                object.put(Body=csv_buffer.getvalue())
                log_count_after_write(endpointID, count_row, finaltablename, schema, database)
            else:
                #What to do if the table exists? replace, append, or fail?
                if_exists = 'replace'
                #Write the data to Snowflake, using pd_writer to speed up loading

                #print(finaltablename)
                with engine.connect().execution_options(autocommit=True) as con:
                    try:
                        sql1 = (f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
                        sqlRes = pd.read_sql(sql1, engine)
                    except:
                        print("error creating schema / doesnt exist")

                    try:
                        sql2 = (f'DROP TABLE {finaltablename};')
                        sqlRes = pd.read_sql(sql2, engine)
                    except:
                        print("error dropping table / doesnt exist")
                        
                    try:
                        print(finaltablename)
                        finaldf.to_sql(
                            name= finaltablename,
                            con=engine, 
                            if_exists=if_exists,
                            method=pd_writer,
                            index=False
                        )
                        print('Dictionary written to Snowflake!')
                        sql = (f'SELECT * FROM {finaltablename};')
                        df_check = pd.read_sql(sql, engine)
                        print(df_check.head(10))
                        log_count_after_write(endpointID, count_row, finaltablename, schema, database)
                    except:
                        print('Issue writing to Snowflake')

    def arrays_2(ti):

        dt = ti.xcom_pull(key="dt", task_ids="Fetch_Data")
        database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
        endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
        schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")
        recLim = ti.xcom_pull(key="inMemoryRecordLimit", task_ids="TokenOffsetTest")

        if customEndpointName:
            endpoint = customEndpointName
        else:
            endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")

        endpoint = endpoint.replace("/", "_")
        local_path=os.path.join(os.getcwd(), 'l2_list_w_dict_DATA.json')
        newPath = tagCustomerID +"/"+ schema +"/" + endpoint + "/" + str(dt) +"/"
        path = tagCustomerID +"/"+ schema +"/" + endpoint + "/" + str(dt) +"/od_keys/"

        #print(path, dt, database, endpoint, schema, local_path, newPath)
        countObj={}
        listCount = []
        listtos3 = {}
        for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=path):

            test = s3.Bucket(s3BucketData).Object(obj2.key).get()
            tmpRead = test['Body'].read().decode('utf-8')
            all = json.loads(tmpRead)
            for x in all:
                newKey = str()

                for y in x:
                    count = 0
                    #print(y)
                    if type(x[y]) is list:
                        for z in x[y]:
                            if type(z) is dict:
                                for a in z:    
                                    if type(z[a]) is list:
                                        for b in z[a]:
                                            if type(b) is dict:
                                                out = {}
                                                for c in b:
                                                    if type(b[c]) is dict:
                                                        for d in b[c]:
                                                            newKey = c + "_"+ d
                                                            out[newKey] = b[c][d]
                                                            #print(out)
                                                    elif type(b[c]) is list:
                                                        continue
                                                    else:
                                                        out[c] = b[c]
                                                if countObj:
                                                    #print(countObj)
                                                    for k in list(countObj):
                                                        newKey = y + "/"+ a 
                                                        #print(k, c)
                                                        if newKey == k and newKey in listCount: 
                                                            #print("if k = a")
                                                            countObj[newKey] += 1
                                                            listtos3[newKey].append(out)  
                                                            print(countObj, listtos3[newKey])
                                                            if (countObj[newKey]/int(recLim)).is_integer() is True:
                                                                #print(listtos3[newKey],"divisible by 5")
                                                                tmpDict={}
                                                                Tpre = endpoint + "_" +k.replace("/", "_")
                                                                tmpDict["TablePrefix"] = Tpre
                                                                tmpDict[newKey] = listtos3[newKey]
                                                                #print("at 50 writing-->", tmpDict)
                                                                file = str(uuid.uuid4())
                                                                with open(local_path, 'w') as f:
                                                                    f.write(json.dumps(tmpDict))
                                                                    print('file Written') ##########FILE KEEP RESETTING At 10 so it gets over written in s3, need different count for files.
                                                                    print(newPath+y+"/"+a+"/"+file)
                                                                    f.close()
                                                                    s3.Bucket(s3BucketData).upload_file(Filename="l2_list_w_dict_DATA.json", Key= newPath+y+"/"+a+"/"+file+".json")
                                                                #print(listtos3[newKey])
                                                                listtos3[newKey] = []
                                                        elif newKey != k and newKey not in listCount:
                                                            #print("if k DOES NOT = a")
                                                            newKey = y + "/"+ a 
                                                            listCount.append(newKey)

                                                            countObj[newKey] = 1
                                                            listtos3[newKey] = [out]
                                                            #print(countObj)
                                                            #print(countObj, listtos3[newKey])
                                                        else:
                                                            print("Adds to nothing")
                                                else:
                                                    #print("countOBJ empty")
                                                    newKey = y + "/"+ a 
                                                    listCount.append(newKey)
                                                    countObj[newKey] = 1
                                                    listtos3[newKey] = [out]
                                                    #print(countObj, listtos3[newKey])

                                        else:
                                            continue

                            else:
                                continue
                    else:
                        continue
        print(countObj)
        for k,v in listtos3.items():
            finalDict={}
            Tpre = endpoint + "_" +k.replace("/", "_")
            finalDict["TablePrefix"] = Tpre
            finalDict[k] = v
            if (countObj[k]/int(recLim)).is_integer() is False:
                #k = k.replace("_", "/")
                file = str(uuid.uuid4())
                print("writing extra to s3", newPath+ k+file)
                with open(local_path, 'w') as f:
                    f.write(json.dumps(finalDict))
                    print('remainder of files Written')
                    f.close()
                    s3.Bucket(s3BucketData).upload_file(Filename="l2_list_w_dict_DATA.json", Key= newPath+k+"/"+file+".json")
            else:
                print("nothing to write to s3")
                continue

    def arrays_2_snowFlake(ti):
        dt = ti.xcom_pull(key="dt", task_ids="Fetch_Data")
        database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
        endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
        schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")
        formatRecordLimit = ti.xcom_pull(key="formatRecordLimit", task_ids="TokenOffsetTest")
        endpointID = ti.xcom_pull(key="endpointID", task_ids="TokenOffsetTest")

        destinationUsername = ti.xcom_pull(key="destinationusername", task_ids="TokenOffsetTest")
        destinationPass = ti.xcom_pull(key="destinationpassword", task_ids="TokenOffsetTest")
        warehouse = ti.xcom_pull(key="warehouse", task_ids="TokenOffsetTest")
        role = ti.xcom_pull(key="role", task_ids="TokenOffsetTest")
        locator = ti.xcom_pull(key="locator", task_ids="TokenOffsetTest")
        region = ti.xcom_pull(key="region", task_ids="TokenOffsetTest")
        destinationType = ti.xcom_pull(key="destinationType", task_ids="TokenOffsetTest")

        if customEndpointName:
            endpoint = customEndpointName
        else:
            endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")

        endpoint = endpoint.replace("/", "_")
        newPath = tagCustomerID +"/"+ schema +"/" + endpoint +"/"+ str(dt)+"/"

        #Create connection to Snowflake using your account and user
        destinationUrl = locator + "." + region
        account_identifier = destinationUrl
        user = destinationUsername
        password = destinationPass
        warehouse = warehouse
        database_name = database.upper()
        schema_name = schema
        role = role
        conn_string = f"snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse}&role={role}"
        engine = create_engine(conn_string)
        #Create your DataFrame
        #listy2 = []

        print(conn_string)
        fList = []

        for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=newPath):
            print("newPath --->", newPath)
            test = s3.Bucket(s3BucketData).Object(obj2.key).get()
            okey = obj2.key
            #print(okey)
            splitd = okey.split("/")
            #print("first->",fList)
            leng = len(splitd)
            #print(leng)
            if len(splitd) == 7:
                fList.append(splitd[4]+"/"+splitd[5])
                fList = list(set(fList))
                for x in fList:
                    if "od_response" in x:
                        fList.remove(x)
                    elif "od_objects" in x:
                        fList.remove(x)
                    elif "od_keys" in x:
                        fList.remove(x)
                    elif ".json" in x:
                        fList.remove(x)
                print(fList)
        for x in fList:
            newPath = tagCustomerID +"/"+ schema +"/" + endpoint +"/" + str(dt) +"/" + x
            #print(newPath)
            finaldf = pd.DataFrame()
            listy = []
            for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=newPath+"/", Delimiter='/'):
                test = s3.Bucket(s3BucketData).Object(obj2.key).get()
                tmpRead = test['Body'].read().decode('utf-8')
                tmp2 = json.loads(tmpRead)
                #print(tmp2)
                #print(test)
                df = pd.DataFrame()
                newtablename = tmp2["TablePrefix"]

                finaltablename = newtablename.upper()
                finaltablename = finaltablename.replace(".", "_")
                finaltablename = finaltablename.replace("/", "_")
                for x in tmp2:
                    if type(tmp2[x]) is list:
                        for y in tmp2[x]:
                            listy.append(y)   
                    #listy2= listy2 + listy
                    #print(listy)
                    tempdf = pd.json_normalize(listy)
                    finaldf = pd.concat([df, tempdf])
                    #print(finaldf)
                    #finaldf = finaldf.rename(lambda x: x.upper(), axis=1)
            finaldf.columns = map(str.upper, finaldf.columns)

            for z in finaldf.columns:
                finaldf = finaldf.rename(columns={z: z.replace(".", "_")})
                finaldf = finaldf.rename(columns={z: z.replace("/", "_")})
                if z == "TABLEPREFIX":
                    finaldf.pop(z)
            
            count_row = finaldf.shape[0]  # Gives number of rows

            if count_row < formatRecordLimit:
                max = count_row
            else:
                max = formatRecordLimit

            pattern = re.compile("^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+|.{0})(?:Z|[+-][01]\d:[0-5]\d)$")
            count = 0
            #print(test)
            dateCols=[]
            while count < max:
                test = finaldf.iloc[count,:]
                for i, v in test.items():
                    if type(v) is str:
                        check = bool(pattern.match(v))
                        if check is True:
                            dateCols.append(i)
                count+=1
                print(count)
            noDupes = list(set(dateCols))
            print(noDupes)
            for col in dateCols:
                #finaldf[col] = pd.to_datetime(finaldf)
                finaldf[col] = pd.to_datetime(finaldf[col])
            

            if destinationType == 'Tableau':
                tabPath = tagCustomerID + "/" + tagAPIID

                object = s3.Object(
                    bucket_name=s3BucketHyper,
                    key=tabPath+'/endpoints/'+finaltablename+'.csv'
                )

                print(finaldf)

                csv_buffer = StringIO()
                finaldf.to_csv(csv_buffer, index=False)
                object.put(Body=csv_buffer.getvalue())
                log_count_after_write(endpointID, count_row, finaltablename, schema, database)
            else:
                #What to do if the table exists? replace, append, or fail?
                if_exists = 'replace'
                #Write the data to Snowflake, using pd_writer to speed up loading
                #print(finaldf)
                #print(finaltablename)
                with engine.connect().execution_options(autocommit=True) as con:
                    try:
                        sql1 = (f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
                        sqlRes = pd.read_sql(sql1, engine)
                    except:
                        print("error creating schema / doesnt exist")

                    try:
                        sql2 = (f'DROP TABLE {finaltablename};')
                        sqlRes = pd.read_sql(sql2, engine)
                    except:
                        print("error dropping table / doesnt exist")
                    try:
                        print(finaltablename)
                        finaldf.to_sql(
                            name= finaltablename,
                            con=engine, 
                            if_exists=if_exists,
                            method=pd_writer,
                            index=False
                        )
                        print('Dictionary written to Snowflake!')
                        sql = (f'SELECT * FROM {finaltablename};')
                        df_check = pd.read_sql(sql, engine)
                        print(df_check.head(10))
                        log_count_after_write(endpointID, count_row, finaltablename, schema, database)
                    except:
                        print('Issue writing to Snowflake')

    def arrays_3(ti):
        dt = ti.xcom_pull(key="dt", task_ids="Fetch_Data")
        database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
        endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
        schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")
        recLim = ti.xcom_pull(key="inMemoryRecordLimit", task_ids="TokenOffsetTest")

        if customEndpointName:
            endpoint = customEndpointName
        else:
            endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")

        endpoint = endpoint.replace("/", "_")


        local_path=os.path.join(os.getcwd(), 'l3_list_w_dict_DATA.json')
        newPath = tagCustomerID +"/"+ schema +"/" + endpoint + "/" + str(dt) +"/"
        path = tagCustomerID +"/"+ schema +"/" + endpoint + "/" + str(dt) +"/od_keys/"

        #print(path, dt, database, endpoint, schema, local_path, newPath)
        countObj={}
        listCount = []
        listtos3 = {}
        #print(recLim, type(recLim))
        for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=path):

            test = s3.Bucket(s3BucketData).Object(obj2.key).get()
            tmpRead = test['Body'].read().decode('utf-8')
            all = json.loads(tmpRead)
            for x in all:
                newKey = str()
                for y in x:
                    count = 0
                    #print(y)
                    if type(x[y]) is list:
                        for z in x[y]:
                            if type(z) is dict:   
                                for a in z:

                                    if type(z[a]) is list:
                                        #print(z[a])
                                        for b in z[a]:
                                            if type(b) is dict:
                                                for c in b:
                                                    if type(b[c]) is list:
                                                        for d in b[c]:
                                                            if type(d) is dict:
                                                                out={}
                                                                for e in d:
                                                                    out[e] = d[e]
                                                                if countObj:
                                                                    #print(countObj)
                                                                    for k in list(countObj):
                                                                        newKey = y + "/"+ a + "/"+c
                                                                        #print(k, c)
                                                                        if newKey == k and newKey in listCount: 
                                                                            #print("if k = a")
                                                                            countObj[newKey] += 1
                                                                            listtos3[newKey].append(out)  
                                                                            print(countObj, listtos3[newKey])
                                                                            if (countObj[newKey]/int(recLim)).is_integer() is True:
                                                                                #print(listtos3[newKey],"divisible by 5")
                                                                                tmpDict={}
                                                                                Tpre = endpoint + "_" +k.replace("/", "_")
                                                                                tmpDict["TablePrefix"] = Tpre
                                                                                tmpDict[newKey] = listtos3[newKey]
                                                                                #print("at 50 writing-->", tmpDict)
                                                                                file = str(uuid.uuid4())
                                                                                with open(local_path, 'w') as f:
                                                                                    f.write(json.dumps(tmpDict))
                                                                                    print('file Written') ##########FILE KEEP RESETTING At 10 so it gets over written in s3, need different count for files.
                                                                                    print(newPath+y+"/"+a+"/"+c+"/"+file)
                                                                                    f.close()
                                                                                    s3.Bucket(s3BucketData).upload_file(Filename="l3_list_w_dict_DATA.json", Key= newPath+y+"/"+a+"/"+c+"/"+file+".json")
                                                                                #print(listtos3[newKey])
                                                                                listtos3[newKey] = []
                                                                        elif newKey != k and newKey not in listCount:
                                                                            #print("if k DOES NOT = a")
                                                                            newKey = y + "/"+ a + "/"+c
                                                                            listCount.append(newKey)

                                                                            countObj[newKey] = 1
                                                                            listtos3[newKey] = [out]
                                                                            #print(countObj)
                                                                            #print(countObj, listtos3[newKey])
                                                                        else:
                                                                            print("Adds to nothing")
                                                                else:
                                                                    #print("countOBJ empty")
                                                                    newKey = y + "/"+ a + "/"+c
                                                                    listCount.append(newKey)
                                                                    countObj[newKey] = 1
                                                                    listtos3[newKey] = [out]
                                                                    #print(countObj, listtos3[newKey])
                                                        else:
                                                            continue
                                                    else:
                                                        continue
                                            else:
                                                continue
                                    else:
                                        continue

                            else:
                                continue

                    else:
                        continue
        for k,v in listtos3.items():
            finalDict={}
            Tpre = endpoint + "_" +k.replace("/", "_")
            finalDict["TablePrefix"] = Tpre
            finalDict[k] = v
            if (countObj[k]/int(recLim)).is_integer() is False:
                #k = k.replace("_", "/")
                file = str(uuid.uuid4())
                print("writing extra to s3", newPath+ k+file)
                with open(local_path, 'w') as f:
                    f.write(json.dumps(finalDict))
                    print('remainder of files Written')
                    f.close()
                    s3.Bucket(s3BucketData).upload_file(Filename="l3_list_w_dict_DATA.json", Key= newPath+k+"/"+file+".json")
            else:
                print("nothing to write to s3")
                continue
  
    def arrays_3_snowFlake(ti):
        dt = ti.xcom_pull(key="dt", task_ids="Fetch_Data")
        database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
        endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
        schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")
        formatRecordLimit = ti.xcom_pull(key="formatRecordLimit", task_ids="TokenOffsetTest")
        endpointID = ti.xcom_pull(key="endpointID", task_ids="TokenOffsetTest")
       
        destinationUsername = ti.xcom_pull(key="destinationusername", task_ids="TokenOffsetTest")
        destinationPass = ti.xcom_pull(key="destinationpassword", task_ids="TokenOffsetTest")
        warehouse = ti.xcom_pull(key="warehouse", task_ids="TokenOffsetTest")
        role = ti.xcom_pull(key="role", task_ids="TokenOffsetTest")
        locator = ti.xcom_pull(key="locator", task_ids="TokenOffsetTest")
        region = ti.xcom_pull(key="region", task_ids="TokenOffsetTest")
        destinationType = ti.xcom_pull(key="destinationType", task_ids="TokenOffsetTest")

        if customEndpointName:
            endpoint = customEndpointName
        else:
            endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")

        endpoint = endpoint.replace("/", "_")

        newPath = tagCustomerID +"/"+ schema +"/" + endpoint +"/"+ str(dt)+"/"

        #Create connection to Snowflake using your account and user
        destinationUrl = locator + "." + region
        account_identifier = destinationUrl
        user = destinationUsername
        password = destinationPass
        warehouse = warehouse
        database_name = database.upper()
        schema_name = schema
        role = role
        conn_string = f"snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse}&role={role}"
        engine = create_engine(conn_string)
        #Create your DataFrame
        #listy2 = []

        #print(conn_string)
        fList = []

        for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=newPath):
            #print("newPath --->", newPath)
            test = s3.Bucket(s3BucketData).Object(obj2.key).get()
            okey = obj2.key
            #print(okey)
            splitd = okey.split("/")
            print(splitd, len(splitd))
            leng = len(splitd)
            #print(leng)
            if len(splitd) == 8:
                #print(splitd[6])
                fList.append(splitd[4]+"/"+splitd[5]+"/"+splitd[6])
                #print(splitd)
                fList = list(set(fList))
                for x in fList:
                    if "od_response" in x:
                        fList.remove(x)
                    elif "od_objects" in x:
                        fList.remove(x)
                    elif "od_keys" in x:
                        fList.remove(x)
                    elif ".json" in x:
                        fList.remove(x)
                print(fList)
        for x in fList:
            newPath = tagCustomerID +"/"+ schema +"/" + endpoint +"/" + str(dt) +"/" + x
            #print(newPath)
            finaldf = pd.DataFrame()
            listy = []
            for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=newPath+"/", Delimiter='/'):
                test = s3.Bucket(s3BucketData).Object(obj2.key).get()
                tmpRead = test['Body'].read().decode('utf-8')
                tmp2 = json.loads(tmpRead)
                print(tmp2)
                #print(test)
                df = pd.DataFrame()
                newtablename = tmp2["TablePrefix"]

                finaltablename = newtablename.upper()
                finaltablename = finaltablename.replace(".", "_")
                finaltablename = finaltablename.replace("/", "_")
                for x in tmp2:
                    if type(tmp2[x]) is list:
                        for y in tmp2[x]:
                            listy.append(y)   
                    #listy2= listy2 + listy
                    #print(listy)
                    tempdf = pd.json_normalize(listy)
                    finaldf = pd.concat([df, tempdf])
                    #print(finaldf)
                    #finaldf = finaldf.rename(lambda x: x.upper(), axis=1)
            finaldf.columns = map(str.upper, finaldf.columns)

            for z in finaldf.columns:
                finaldf = finaldf.rename(columns={z: z.replace(".", "_")})
                finaldf = finaldf.rename(columns={z: z.replace("/", "_")})
                if z == "TABLEPREFIX":
                    finaldf.pop(z)
            
            count_row = finaldf.shape[0]  # Gives number of rows

            if count_row < formatRecordLimit:
                max = count_row
            else:
                max = formatRecordLimit

            pattern = re.compile("^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+|.{0})(?:Z|[+-][01]\d:[0-5]\d)$")
            count = 0
            #print(test)
            dateCols=[]
            while count < max:
                test = finaldf.iloc[count,:]
                for i, v in test.items():
                    if type(v) is str:
                        check = bool(pattern.match(v))
                        if check is True:
                            dateCols.append(i)
                count+=1
                print(count)
            noDupes = list(set(dateCols))
            print(noDupes)
            for col in dateCols:
                #finaldf[col] = pd.to_datetime(finaldf)
                finaldf[col] = pd.to_datetime(finaldf[col])
            
            if destinationType == 'Tableau':
                tabPath = tagCustomerID + "/" + tagAPIID

                object = s3.Object(
                    bucket_name=s3BucketHyper,
                    key=tabPath+'/endpoints/'+finaltablename+'.csv'
                )

                print(finaldf)

                csv_buffer = StringIO()
                finaldf.to_csv(csv_buffer, index=False)
                object.put(Body=csv_buffer.getvalue())
                log_count_after_write(endpointID, count_row, finaltablename, schema, database)
            else:

                #What to do if the table exists? replace, append, or fail?
                if_exists = 'replace'
                #Write the data to Snowflake, using pd_writer to speed up loading
                #print(database_name)
                #print(finaltablename)
                with engine.connect().execution_options(autocommit=True) as con:
                    try:
                        sql1 = (f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
                        sqlRes = pd.read_sql(sql1, engine)
                    except:
                        print("error creating schema / doesnt exist")

                    try:
                        sql2 = (f'DROP TABLE {finaltablename};')
                        sqlRes = pd.read_sql(sql2, engine)
                    except:
                        print("error dropping table / doesnt exist")

                    try:
                        print(finaltablename)
                        finaldf.to_sql(
                            name= finaltablename,
                            con=engine, 
                            if_exists=if_exists,
                            method=pd_writer,
                            index=False
                        )
                        print('Dictionary written to Snowflake!')
                        sql = (f'SELECT * FROM {finaltablename};')
                        df_check = pd.read_sql(sql, engine)
                        print(df_check.head(10))
                        log_count_after_write(endpointID, count_row, finaltablename, schema, database)
                    except:
                        print('Issue writing to Snowflake')

    def arrays_4(ti):

        dt = ti.xcom_pull(key="dt", task_ids="Fetch_Data")
        database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
        endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
        schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")
        recLim = ti.xcom_pull(key="inMemoryRecordLimit", task_ids="TokenOffsetTest")

        if customEndpointName:
            endpoint = customEndpointName
        else:
            endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")

        endpoint = endpoint.replace("/", "_")

        endpoint = endpoint.replace("/", "_")
        local_path=os.path.join(os.getcwd(), 'l4_list_w_dict_DATA.json')
        newPath = tagCustomerID +"/"+ schema +"/" + endpoint + "/" + str(dt) +"/"
        path = tagCustomerID +"/"+ schema +"/" + endpoint + "/" + str(dt) +"/od_keys/"


        #print(path, dt, database, endpoint, schema, local_path, newPath)

        for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=path):

            test = s3.Bucket(s3BucketData).Object(obj2.key).get()
            #print(test)
            tmpRead = test['Body'].read().decode('utf-8')
            all = json.loads(tmpRead)
            for x in all:
                for y in x: #level 1 keys ex. 'status', 'name', 'naics', etc.                              
                    if type(x[y]) is list: #{[]}
                        #print(x[y])
                        for z in x[y]:
                            #print(z)
                            if type(z) is dict:# level 1 list with dict
                                for a in z:
                                    #print(z[a])
                                    if type(z[a]) is dict: #This is body { paging{}, paystatements{}}
                                        #print(z[a])
                                        for b in z[a]:
                                            if type(z[a][b]) is list:
                                                count = 0
                                                #print(json.dumps(z[a][b]))
                                                #print(json.dumps(x))
                                                #print(b)
                                                #print(json.dumps(z[a][b]))
                                                for c in z[a][b]:
                                                    count +=1
                                                    if type(c) is dict:
                                                        newc = {}
                                                        #print(c)
                                                        for d in c:
                                                            if type(c[d]) is list:
                                                                continue
                                                            else:
                                                                newc[d] = c[d]
                                                        c = newc
                                                        #print(c)
                                                        with open(local_path, 'w') as f:
                                                            f.write(json.dumps(c))
                                                            f.close()
                                                            s3.Bucket(s3BucketData).upload_file(Filename="l4_list_w_dict_DATA.json", Key= newPath+y+"/"+a+"/"+b+"/_/"+str(count)+".json")   
                                                            print("file written to s3")
                                                            print(json.dumps(c))
                                                #    print(c)
                                                #    #listdex6 +=1
                                                #    #o2 ={"TablePrefix": endpoint+"_"+y+"_"+a+"_"+b, endpoint+"_"+y+"_"+a+"_ID": listdex4, endpoint+"_"+y+"_"+a+"_"+b+"_ID": listdex6}
                                                #    #c = {**o2, **c}
                                                #    #listed6.append(c)
                                                ##z[a][b] = listed6
                                                ##print(z[a][b])
                                            elif type(z[a][b]) is dict:
                                                continue
                                                #out = {}
                                                #for c in z[a][b]:
                                                    #would have to check for type here to make sure it is not dict or list
                                                    #newKey = a+"_"+b+"_"+c
                                                    #out[newKey] = z[a][b][c]
                                                #print(out)                                       
                                                #z[a] = {**out, **z[a]}
                                                #print(z[a])
                                                #print("keys ------>", a, b )
                                            else:
                                                continue
                                                print("Not list or dict ------>", z[a][b] )
                                    else:
                                          continue
                            else:
                               continue 
                           
                            #with open(local_path, 'w') as f:
                            #    f.write(json.dumps(x))
                            #    f.close()
                            #    s3.Bucket(s3BucketData).upload_file(Filename="l3_list_w_dict_DATA.json", Key= newPath+y+"/"+a+"/"+c+"/"+str(count)+".json")   
                            #    print("file written to s3")
                            #print(json.dumps(x))

    def arrays_4_snowFlake(ti):
        dt = ti.xcom_pull(key="dt", task_ids="Fetch_Data")
        database = ti.xcom_pull(key="database", task_ids="TokenOffsetTest")
        endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")
        customEndpointName = ti.xcom_pull(key="customEndpointName", task_ids="TokenOffsetTest")
        schema = ti.xcom_pull(key="schema", task_ids="TokenOffsetTest")
        recLim = ti.xcom_pull(key="inMemoryRecordLimit", task_ids="TokenOffsetTest")
        formatRecordLimit = ti.xcom_pull(key="formatRecordLimit", task_ids="TokenOffsetTest")
        endpointID = ti.xcom_pull(key="endpointID", task_ids="TokenOffsetTest")

        destinationUsername = ti.xcom_pull(key="destinationusername", task_ids="TokenOffsetTest")
        destinationPass = ti.xcom_pull(key="destinationpassword", task_ids="TokenOffsetTest")
        warehouse = ti.xcom_pull(key="warehouse", task_ids="TokenOffsetTest")
        role = ti.xcom_pull(key="role", task_ids="TokenOffsetTest")
        locator = ti.xcom_pull(key="locator", task_ids="TokenOffsetTest")
        region = ti.xcom_pull(key="region", task_ids="TokenOffsetTest")
        destinationType = ti.xcom_pull(key="destinationType", task_ids="TokenOffsetTest")

        if customEndpointName:
            endpoint = customEndpointName
        else:
            endpoint = ti.xcom_pull(key="endpoint", task_ids="TokenOffsetTest")

        endpoint = endpoint.replace("/", "_")
        newPath = tagCustomerID +"/"+ schema +"/" + endpoint +"/"+ str(dt)+"/"


        #Create connection to Snowflake using your account and user
        destinationUrl = locator + "." + region
        account_identifier = destinationUrl
        user = destinationUsername
        password = destinationPass
        warehouse = warehouse
        database_name = database.upper()
        schema_name = schema
        role = role
        conn_string = f"snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse}&role={role}"
        engine = create_engine(conn_string)
        #Create your DataFrame
        #listy2 = []

        #print(conn_string)
        fList = []

        for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=newPath):
            #print("newPath --->", newPath)
            test = s3.Bucket(s3BucketData).Object(obj2.key).get()
            okey = obj2.key
            #print(okey)
            splitd = okey.split("/")
            leng = len(splitd)
            #print(leng)
            if len(splitd) == 9:
                #print(splitd[6])
                fList.append(splitd[4]+"/"+splitd[5]+"/"+splitd[6]+"/"+splitd[7])
                #print(splitd)
                fList = list(set(fList))
                #print("flist before --> ", fList)
                for x in fList:
                    if "od_response" in x:
                        fList.remove(x)
                    elif "od_objects" in x:
                        fList.remove(x)
                    elif "od_keys" in x:
                        fList.remove(x)
                    elif ".json" in x:
                        fList.remove(x)
                #print(fList)
        for x in fList:
            newPath = tagCustomerID +"/"+ schema +"/" + endpoint +"/" + str(dt) +"/" + x
            #print(newPath)
            finaldf = pd.DataFrame()
            listy = []
            for obj2 in s3.Bucket(s3BucketData).objects.filter(Prefix=newPath+"/", Delimiter='/'):
                test = s3.Bucket(s3BucketData).Object(obj2.key).get()
                tmpRead = test['Body'].read().decode('utf-8')
                tmp2 = json.loads(tmpRead)
                #print(tmp2["TablePrefix"])
                #print(test)
                df = pd.DataFrame()
                newtablename = tmp2["TablePrefix"]

                finaltablename = newtablename.upper()
                finaltablename = finaltablename.replace(".", "_")
                finaltablename = finaltablename.replace("/", "_")
                print(tmp2)
                listy.append(tmp2)
                tempdf = pd.json_normalize(listy)
                finaldf = pd.concat([df, tempdf])
                #print(finaldf)
                #finaldf = finaldf.rename(lambda x: x.upper(), axis=1)
            finaldf.columns = map(str.upper, finaldf.columns)

            for z in finaldf.columns:
                finaldf = finaldf.rename(columns={z: z.replace(".", "_")})
                finaldf = finaldf.rename(columns={z: z.replace("/", "_")})
                if z == "TABLEPREFIX":
                    finaldf.pop(z)
            
            count_row = finaldf.shape[0]  # Gives number of rows

            if count_row < formatRecordLimit:
                max = count_row
            else:
                max = formatRecordLimit

            pattern = re.compile("^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+|.{0})(?:Z|[+-][01]\d:[0-5]\d)$")
            count = 0
            #print(test)
            dateCols=[]
            while count < max:
                test = finaldf.iloc[count,:]
                for i, v in test.items():
                    if type(v) is str:
                        check = bool(pattern.match(v))
                        if check is True:
                            dateCols.append(i)
                count+=1
                print(count)
            noDupes = list(set(dateCols))
            print(noDupes)
            for col in dateCols:
                #finaldf[col] = pd.to_datetime(finaldf)
                finaldf[col] = pd.to_datetime(finaldf[col])

            if destinationType == 'Tableau':
                tabPath = tagCustomerID + "/" + tagAPIID

                object = s3.Object(
                    bucket_name=s3BucketHyper,
                    key=tabPath+'/endpoints/'+finaltablename+'.csv'
                )

                print(finaldf)

                csv_buffer = StringIO()
                finaldf.to_csv(csv_buffer, index=False)
                object.put(Body=csv_buffer.getvalue())  
                log_count_after_write(endpointID, count_row, finaltablename, schema, database)
            else:

                #What to do if the table exists? replace, append, or fail?
                if_exists = 'replace'
                #Write the data to Snowflake, using pd_writer to speed up loading
                #print(finaldf)
                #print(finaltablename)
                with engine.connect().execution_options(autocommit=True) as con:
                    try:
                        sql1 = (f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
                        sqlRes = pd.read_sql(sql1, engine)
                    except:
                        print("error creating schema / doesnt exist")

                    try:
                        sql2 = (f'DROP TABLE {finaltablename};')
                        sqlRes = pd.read_sql(sql2, engine)
                    except:
                        print("error dropping table / doesnt exist")
                        
                    try:
                        print(finaltablename)
                        finaldf.to_sql(
                            name= finaltablename,
                            con=engine, 
                            if_exists=if_exists,
                            method=pd_writer,
                            index=False
                        )
                        print('Dictionary written to Snowflake!')
                        sql = (f'SELECT * FROM {finaltablename};')
                        df_check = pd.read_sql(sql, engine)
                        print(df_check.head(10))
                        log_count_after_write(endpointID, count_row, finaltablename, schema, database)
                    except:
                        print('Issue writing to Snowflake')

    def log_endpoint_complete(ti, **kwargs):
        endpointID = ti.xcom_pull(key="endpointID", task_ids="TokenOffsetTest")

        url = vinylDomUrl+"/endpointlog"
        context = kwargs
        run_id = context['dag_run'].run_id


        payload = json.dumps({
          "endpointID": endpointID,
          "log": "Job Completed Successfully",
          "runID": run_id,
          "EndpointLogTypeID": "525be0fe-374c-4332-9e1f-2162b601c190"
        })

        headers = {
          'X-API-KEY': vinylApiKey,
          'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        print(response.text)

    tagCustomerID =config['tagCustomerID']
    tagCustomer = config['tagCustomer']
    tagAPIID =config['tagAPIID']
    tagAPI =config['tagAPI']
    tagEndpointID =config['tagEndpointID']
    tagEndpoint =config['tagEndpoint']
    tagVinyl=config['tagVinyl']

    tags = list()
    tags.extend([tagCustomerID,tagCustomer,tagAPIID,tagAPI,tagEndpointID,tagEndpoint,tagVinyl])

    with DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              is_paused_upon_creation=False,
              tags=tags

    ) as dag:

        TokenOffsetTest = PythonOperator(
            task_id='TokenOffsetTest',
            python_callable=tokenoffsettest,
            op_kwargs={"config":config},
        )

        Fetch_Data = PythonOperator(
            task_id='Fetch_Data',
            python_callable=fetch_data,
            retries=1
        )

        KeyRelationshipS = PythonOperator(
            task_id='KeyRelationshipS',
            python_callable=keyrelationships,
            retries=3,
            retry_delay=5,
            retry_exponential_backoff=True,
        )

        Objects_1_2 = PythonOperator(
            task_id='Objects_1_2',
            python_callable=objects_1_2,
            retries=3,
            retry_delay=5,
            retry_exponential_backoff=True,
        )

        Objects_1_2_SnowFlake = PythonOperator(
            task_id='Objects_1_2_SnowFlake',
            python_callable=objects_1_2_snowFlake,
            execution_timeout=timedelta(hours=2),
            retries=2,
            retry_delay=5,
            retry_exponential_backoff=True,
        )

        Arrays_1 = PythonOperator(
            task_id='Arrays_1',
            python_callable=arrays_1,
            retries=3,
            retry_delay=5,
            retry_exponential_backoff=True,
        )

        Arrays_1_SnowFlake = PythonOperator(
            task_id='Arrays_1_SnowFlake',
            python_callable=arrays_1_snowFlake,
            retries=3,
            retry_delay=5,
            retry_exponential_backoff=True,
        )

        Arrays_2 = PythonOperator(
            task_id='Arrays_2',
            python_callable=arrays_2,
            retries=3,
            retry_delay=5,
            retry_exponential_backoff=True,
        )

        Arrays_2_SnowFlake = PythonOperator(
            task_id='Arrays_2_SnowFlake',
            python_callable=arrays_2_snowFlake,
            retries=3,
            retry_delay=5,
            retry_exponential_backoff=True,
        )

        Arrays_3 = PythonOperator(
            task_id='Arrays_3',
            python_callable=arrays_3,
            retries=3,
            retry_delay=5,
            retry_exponential_backoff=True,
        )

        Arrays_3_SnowFlake = PythonOperator(
            task_id='Arrays_3_SnowFlake',
            python_callable=arrays_3_snowFlake,
            retries=3,
            retry_delay=5,
            retry_exponential_backoff=True,
        )

        Arrays_4 = PythonOperator(
            task_id='Arrays_4',
            python_callable=arrays_4,
            retries=3,
            retry_delay=5,
            retry_exponential_backoff=True,
        )
   
        Arrays_4_SnowFlake = PythonOperator(
            task_id='Arrays_4_SnowFlake',
            python_callable=arrays_4_snowFlake,
            retries=3,
            retry_delay=5,
            retry_exponential_backoff=True,
        )

        Log_Endpoint_Complete = PythonOperator(
            task_id='Log_Endpoint_Date',
            python_callable=log_endpoint_complete,
            retries=3,
            retry_delay=5,
            retry_exponential_backoff=True,
        )


        TokenOffsetTest >> Fetch_Data >> KeyRelationshipS >> Objects_1_2 >> Objects_1_2_SnowFlake >> Arrays_1 >> Arrays_1_SnowFlake >> Arrays_2 >> Arrays_2_SnowFlake >> Arrays_3 >> Arrays_3_SnowFlake >> Arrays_4 >> Arrays_4_SnowFlake >> Log_Endpoint_Complete

    return dag


#def all_to_s3():

s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id=awsAccessKey,
    aws_secret_access_key=awsSecretKey
)
countUrl = vinylDomUrl+"/staticlimit?$count=true&$limit=0"
headers = {
    'Content-Type': 'application/json',
    'X-API-Key': vinylApiKey
}
imports = "from airflow import DAG\nfrom airflow.operators.python_operator import PythonOperator\n#from airflow.providers.mysql.operators.mysql import MySqlOperator\nfrom datetime import datetime\nimport requests\nimport json\nfrom locale import normalize\nfrom wsgiref import headers\nimport sys\nimport math\nfrom datetime import datetime, timedelta\nfrom snowflake.connector.pandas_tools import pd_writer\nimport pandas as pd\nimport pandas.api.types\nfrom sqlalchemy import create_engine\nfrom sqlalchemy import NVARCHAR\nfrom airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API\nfrom airflow.models.xcom import XCom\nfrom requests.auth import HTTPBasicAuth\nfrom io import StringIO\nfrom botocore.exceptions import ClientError\nfrom botocore.client import Config\nimport os\nfrom airflow.operators.trigger_dagrun import TriggerDagRunOperator\nfrom airflow.providers.http.operators.http import SimpleHttpOperator\nfrom airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator\nimport boto3\nfrom airflow.models import Variable\nimport uuid \nimport re\nimport inspect\nimport pantab"
countResponse = requests.request("GET", countUrl, headers=headers)
countRes = json.loads(countResponse.content)
print(countRes)
count = countRes['count']
limit = 100
url = vinylDomUrl+"/staticlimit?$count=true&$limit="+str(limit)
totalIterations = math.ceil(int(count) / int(limit))
currentIterations = 0
newOffset = 0
for x in range (0, totalIterations):
    if currentIterations <= totalIterations:
        newUrl = url + "&$offset=" + str(newOffset)
        response = requests.request("GET", newUrl, headers=headers)
        res = json.loads(response.content)
        print(res)
        configList = res['items']
        
        default_args = {'start_date':datetime.now()}
        for config in configList:
            dag_id = config['dag']
            schedule= config['schedule']
            #object = s3.Object(
            #    bucket_name=s3BucketDag,
            #    key='dags/'+dag_id+'.py'
            #)
            #configStr = "config = "+ str(config)
            #dagIDStr = "dag_id = " + '"' + str(dag_id) + '"'
            #scheduleStr = "schedule = " + '"' + schedule + '"'
            #defaultArgsStr = "default_args = {'start_date':datetime.now()}"
            ##data_string = "print("'"'+ config['endpointID']+'"'")"
            #data_string = imports+"\n\n"+configStr+"\n"+dagIDStr+"\n"+scheduleStr+"\n"+defaultArgsStr+"\n\n"+inspect.getsource(create_dag)+"\n"+str('create_dag(dag_id, schedule, default_args, config)')
            #if env == "prod":
            #    object.put(Body=data_string)
            #else:
            #    print("ENV VAR IS DEV --- Files not written if not in local")
            globals()[dag_id] = create_dag(dag_id, schedule, default_args, config)
        currentIterations+=1
        newOffset = currentIterations * int(limit)

with DAG(
    dag_id = 'Dynamic_noAuth_staticLimit_dev',
    start_date = datetime(2022, 2, 12),
    catchup = False,
    schedule_interval='@once'
) as dag: 
    
    createDag = PythonOperator(
        task_id='createDag',
        python_callable=create_dag,
    )

createDag


#    allToS3 = PythonOperator(
#        task_id='allToS3',
#        python_callable=all_to_s3,
#    )
#
#allToS3
