import time

import cassandra
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

CLIENT_ID = 'lGeuDfljFgYSjzlazKSyThWj'
CLIENT_SECRET = "U,a+pF7CW2jh1pM5yC+BkgQ03CUlvvGrloGtJ0aEwB59OA2RZuYMNB6AbyC.aM+p.d3rW5kNM-4pvv_j2AgwDnUfwy_4l,A5b+ur-Mer5vqRQlPBX,,LRK3h9JoKJfta"
LOCATION = 'D:\\cassandra_bundle\\secure-connect-major-project.zip'


class CassandraManager:
    def __init__(self, location, client_id, client_secret):
        try:
            self.cloud_config = {'secure_connect_bundle': location}
            self.auth_provider = PlainTextAuthProvider(client_id, client_secret)
            self.cluster = Cluster(cloud=self.cloud_config, auth_provider=self.auth_provider)
        except Exception as e:
            raise Exception(f"(__init__): Something went wrong on initiation process\n" + str(e))

    def connect_session(self):
        try:
            session = self.cluster.connect()
            if not session.is_shutdown:
                print("session is initialized")
                return session
            else:
                return "server is not connected"
        except Exception as e:
            raise Exception("something unusual happend during session connecting " + str(e))

    def create_keyspace(self, keyspace_name):
        try:
            session = self.connect_session()
            session.execute("CREATE KEYSPACE test_keyspace WITH replication ={'Class': 'SimplyStrategy', "
                            "'asia-south1': '3'}  AND durable_writes = true;")

        except Exception as e:
            raise Exception("An error occured while creating keyspace " + str(e))

    def create_table(self, table_name):
        try:
            session = self.connect_session()
            session.execute(
                f"create table if not exists scrapper.{table_name}(product_name text , product_searched text, price "
                f"text, discount_percent text, ratings text, comments text, customer_name text, "
                f"PRIMARY KEY(product_name,customer_name))")
            print(f"{table_name} is created")
        except Exception as e:
            raise Exception("An error occured while creating table " + str(e))

    def insert_one_row_to_table(self, table_name, values):
        try:
            print("value insertion has started")
            session = self.connect_session()
            session.execute(
                f"INSERT INTO scrapper.{table_name}(product_name,customer_name,comments,discount_percent,"
                f"price,product_searched,ratings) VALUES{values}")
            print("values inserted successfully")
        except Exception as e:
            raise Exception("An error occured while inserting values " + str(e))

    def check_table_is_present(self, table_name):
        try:
            session = self.connect_session()
            data = session.execute(f"select * from scrapper.{table_name}")
            return 1
        except cassandra.InvalidRequest as e:
            return 0

    def fetch_data_from_table(self, table_name):
        try:
            session = self.connect_session()
            data = session.execute(f"select * from scrapper.{table_name}")
            return data
        except cassandra.InvalidRequest as e:
            raise Exception("An error occured during fetching of data " + str(e))

    def converting_data_into_list(self, table_name):
        try:
            session = self.connect_session()
            data = session.execute(f"select * from scrapper.{table_name}")
            time.sleep(5)
            lst = [i for i in data]
            print(len(lst))
            s = pd.DataFrame(lst)
            final_lst = []
            data1 = {}
            keys = list(s.columns)
            for i in range(len(lst)):
                for j in range(len(s.iloc[i])):
                    data1[keys[j]] = s.iloc[i][j]
                final_lst.append(data1)
            return final_lst
        except Exception as e:
            raise Exception("An error occured while formatting data into iterable form " + str(e))

    def table_query(self, table_name, customer_name, product_name):
        try:
            session = self.connect_session()
            q = f"select * from scrapper.{table_name} where customer_name='{customer_name}' and product_name='{product_name}'"
            print(q)
            data = session.execute(q)
            data1 = [i for i in data]
            if data1 == []:
                return False
            data2 = pd.DataFrame(data1)
            if customer_name in list(data2["customer_name"]):
                return True
            else:
                return False

        except Exception as e:
            raise Exception("An error occured while return table_query " + str(e))

    def bulk_inserting(self, table_name):
        try:
            print("bulk inserting operation has started")
            session = self.connect_session()
            q = f"COPY scrapper.{table_name}(product_name,customer_name,comments,discount_percent,price,product_searched,ratings) FROM 'test.csv' WITH DELIMITER=',' AND HEADER=FALSE;"
            session.execute(q)
            return 1
        except Exception as e:
            raise Exception("An error occured during bulk insertion")
