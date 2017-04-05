#!/usr/bin/python
#you can execute this script with the 'python document.py' command
#first you want to map the table that you want to write to ('document' by default) then ensure that you uncomment the lines near the bottom to insert the data into the tbale

import MySQLdb
import time
from time import gmtime, strftime

tableName = 'document'
# Open database connection
db = MySQLdb.connect("127.0.0.1","root","root","latipay" )

# prepare a cursor object using cursor() method
cursor = db.cursor()
cursor2 = db.cursor()
cursor.execute("SELECT code FROM latipay.merchant_base;")
#generate a list of all merchants in the DB
merchants = list(cursor.fetchall())

baseQuery = "SELECT id_number, id_name, id_type, register_date as issue_date, expire_date as expiration_date, merchant_code, register_address as place_of_issue FROM latipay.paper WHERE merchant_code="
#get id_number for each merchant_id
for merchant_code in merchants:
    merchant_code = merchant_code[0]
    merchant_code = "'" + merchant_code + "'"
    cursor2.execute(baseQuery + merchant_code +';')
    doc_info = cursor2.fetchone()
    if doc_info is not None and doc_info[1] != '' and doc_info[1] is not None:
        id_number = doc_info[0]
        id_name = doc_info[1]
        id_type = doc_info[2]
        if doc_info[3] is not None and doc_info[3] != '':
            issue_date = time.strftime("%Y:%m:%d")
        else: issue_date = doc_info[3]
        if doc_info[4] is not None and doc_info[4] != '':
            expiration_date = time.strftime("%Y:%m:%d")
            print expiration_date
        else: expiration_date = doc_info[4]
        merchant_code = doc_info[5]
        place_of_issue = doc_info[6]



    # insert1 = "INSERT INTO" + tableName + "(id_number, merchant_code, id_name, issue_date, expiration_date, place_of_issue) VALUES ("
    # insert2 = id_number + ',' + merchant_code + ',' + id_name + ',' + issue_date + ',' + expiration_date + ',' + place_of_issue");"
    # insertQuery = insert1 + insert2
    # insertCursor = db.cursor()
    # insertCursor.execute(insertQuery)
    # db.commit()

print ('success!')

# disconnect from server
db.close()
