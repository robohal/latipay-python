#!/usr/bin/python

import MySQLdb
from elasticsearch import Elasticsearch
es = Elasticsearch()

tableName = 'test4'
# Open database connection
db = MySQLdb.connect("127.0.0.1","root","root","latipay" )

# prepare a cursor object using cursor() method
cursor = db.cursor()
cursor2 = db.cursor()

#create table with name = tableName
# cursor28.execute("CREATE TABLE test5 (merchant_code VARCHAR(255), merchant_name VARCHAR(255), merchant_address_street VARCHAR(255), merchant_address_city VARCHAR(255), merchant_address_province VARCHAR(255), merchant_address_country VARCHAR(255), merchant_email VARCHAR(255), merchant_phone VARCHAR(255), merchant_comment VARCHAR(255), merchant_type_text VARCHAR(255), merchant_industry_text VARCHAR(255), merchant_physical_presence_text VARCHAR(255), merchant_residence_text VARCHAR(255), merchant_incorporation_text VARCHAR(255), merchant_trade_country_text VARCHAR(255), merchant_product_text VARCHAR(255));")
# execute SQL query using execute() method.
cursor.execute("SELECT `COLUMN_NAME`  FROM `INFORMATION_SCHEMA`.`COLUMNS`  WHERE `TABLE_SCHEMA`='latipay' AND `TABLE_NAME`='receiver_order';")

columns = list(cursor.fetchall())
# baseQuery = "SELECT name FROM latipay.merchant_base WHERE code="
# baseQuery3 = "SELECT email FROM latipay.merchant_base WHERE code="
# baseQuery4 = "SELECT tel FROM latipay.merchant_base WHERE code="

print columns

#get merchant_name for each merchant_id
# row = 0
# for merchant_code in merchants:
#     print ('processing: ' + str(row))

#     #query variables from legacy DB and convert to strings
#     cursor2.execute(baseQuery + merchant_code +';')
#     merchant_name = cursor2.fetchone()
#     if merchant_name != None:
#         merchant_name = "'" + merchant_name[0] + "'"
#     else: merchant_name = "null"
    

#     #tracks progress
#     row = row + 1

#     insert1 = "INSERT INTO test6 (merchant_code, merchant_name, merchant_address_street, merchant_address_city, merchant_address_province, merchant_address_country, merchant_email, merchant_phone, merchant_comment, merchant_type_text, merchant_industry_text, merchant_physical_presence_text, merchant_residence_text, merchant_incorporation_text, merchant_trade_country_text, merchant_product_text) VALUES ("
#     insert2 = merchant_code + ',' + merchant_name + ',' + merchant_address_street + ',' + merchant_address_city + ',' + merchant_address_province + ',' + merchant_address_country + ',' + merchant_email + ',' + merchant_phone + ',' + merchant_comment + ',' + merchant_type_text + ',' + merchant_industry_text + ',' + merchant_physical_presence_text + ',' + merchant_residence_text + ',' + merchant_incorporation_text + ',' + merchant_trade_country_text + ',' + merchant_product_text + ");"
#     insertQuery = insert1 + insert2
    
#     cursor29.execute(insertQuery)
#     db.commit()

    # if row > 20:
    #     break

print ('success!')

# disconnect from server
db.close()
