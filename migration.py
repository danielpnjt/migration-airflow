from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.hooks.mysql_hook import MySqlHook

import pymongo
import logging
import mysql
import mysql.connector

def connect_mongo():
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db = client["Jatis_Mobile"]
    col = db["Jatis_Mobile"]
    return col

def fun_count(**kwargs):
    #read data
    col = connect_mongo()
    data = col.find()
    counting = 0
    logging.info(data)
    for item in enumerate(data):
        counting = counting + 1
    if counting>0:
        logging.info('already data. continue')
        return True
    else:
        logging.info('no data. stopping')
        return False

def fun_migrate(**kwargs):
    #read data
    col = connect_mongo()
    data = col.find()

    #transform
    item_id = []
    client_id = []
    reference_id = []
    whatsapp_id = []
    contacts_name = []
    contacts_wa_id = []
    messages_type = []
    messages_from = []
    messages_id = []
    messages_text = []
    messages_timestamp = []
    result_status = []
    result_message = []
    created_at = []

    for item in data:
        item_id.append(item['_id'])
        client_id.append(item['client_id'])
        reference_id.append(item['reference_id'])
        whatsapp_id.append(item['whatsapp_id'])
        contacts_name.append(item['contacts'][0]['profile']['name'])
        contacts_wa_id.append(item['contacts'][0]['wa_id'])
        messages_type.append(item['messages']['type'])
        messages_from.append(item['messages']['from'])
        messages_id.append(item['messages']['id'])
        messages_text.append(item['messages']['text']['body'])
        messages_timestamp.append(item['messages']['timestamp'])
        result_status.append(item['result']['status'])
        result_message.append(item['result']['message'])
        created_at.append(item['created_at'])
    
    #migrate
    #src = MySqlHook(mysql_conn_id='mysql-dw')
    #conn = src.get_conn()
    conn = mysql.connector.connect(host = 'localhost', user = 'root', 
                               password = 'mysecretpassword', database = 'db_jatis')
    cursor = conn.cursor()
    for i in range(0, len(item_id)):
        cursor.execute("""INSERT IGNORE INTO data_jatis(id, client_id, reference_id, 
                    whatsapp_id, contacts_name, contacts_wa_id, messages_type, 
                    messages_from, messages_id, messages_text, messages_timestamp, 
                    result_status, result_message, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (item_id[i], client_id[i], reference_id[i], whatsapp_id[i], 
                            contacts_name[i], contacts_wa_id[i], messages_type[i], 
                            messages_from[i], messages_id[i], messages_text[i], 
                            messages_timestamp[i], result_status[i], result_message[i], 
                            created_at[i]
                            ))
        conn.commit()
        #delete from mongodb
        col.delete_one({'_id':item_id[i]})
        

with DAG(dag_id="migration_dag", start_date=datetime(2022,1,1), 
         schedule_interval="@daily", catchup=False) as dag:
    task1 = PythonOperator(task_id="count", python_callable=fun_count)
    task2 = PythonOperator(task_id="migrate", python_callable=fun_migrate)

    task1>>task2