import mysql.connector as conn
import csv
from AppFlow.constant import *
from AppFlow.exception import CreditcardException
import sys, os, io
import requests
import pandas as pd

class Database_Creation:
    def __init__(self):
        self.dataset_download_url = Dataset_download_raw_url
        self.raw_dataset_file_path = RAW_DATASET_FILE_PATH

    def database_create(self):
        try:
            myobj = conn.connect(host='localhost', user='root', passwd='MySql')
            cursor = myobj.cursor()

            cursor.execute("SHOW DATABASES")
            list_of_databases = cursor.fetchall()
            print(list_of_databases)
            if (('ccdp',) in list_of_databases):
                print("Exists")
                return False
            
            cursor.execute("CREATE DATABASE ccdp")
            cursor.execute("USE ccdp")
            cursor.execute("""
                CREATE TABLE creditcardD(
                    ID VARCHAR(20), LIMIT_BAL VARCHAR(20), SEX VARCHAR(20), EDUCATION VARCHAR(20),
                    MARRIAGE VARCHAR(20), AGE VARCHAR(20), PAY_1 VARCHAR(20), PAY_2 VARCHAR(20),
                    PAY_3 VARCHAR(20), PAY_4 VARCHAR(20), PAY_5 VARCHAR(20), PAY_6 VARCHAR(20),
                    BILL_AMT1 VARCHAR(20), BILL_AMT2 VARCHAR(20), BILL_AMT3 VARCHAR(20), BILL_AMT4 VARCHAR(20),
                    BILL_AMT5 VARCHAR(20), BILL_AMT6 VARCHAR(20), PAY_AMT1 VARCHAR(20), PAY_AMT2 VARCHAR(20),
                    PAY_AMT3 VARCHAR(20), PAY_AMT4 VARCHAR(20), PAY_AMT5 VARCHAR(20), PAY_AMT6 VARCHAR(20),
                    Default_Prediction VARCHAR(30)
                )
            """)
            cursor.execute("SHOW TABLES")
            print(cursor.fetchall())

            # Downloading the raw file to a CSV file using requests
            response = requests.get(self.dataset_download_url)
            response.raise_for_status()
            with open(self.raw_dataset_file_path, 'wb') as file:
                file.write(response.content)
            
            count = 0
            with open(self.raw_dataset_file_path, 'r') as f:
                cd = csv.reader(f, delimiter=',')
                for i in cd:
                    count += 1
                    if count == 1:
                        rows = str(i[0])
                        cursor.execute(f'INSERT INTO creditcardD VALUES ({chr(34)}ID{chr(34)}{rows[2:]})')
                        print(f'INSERT INTO creditcardD VALUES ({chr(34)}ID{chr(34)}{rows[2:]})')
                        continue

                    t = i
                    values = ','.join([f'"{x}"' for x in t])
                    cursor.execute(f'INSERT INTO creditcardD VALUES({values})')

            print("Total rows affected", count)
            myobj.commit()
            return True
        except Exception as e:
            raise CreditcardException(e, sys) from e
