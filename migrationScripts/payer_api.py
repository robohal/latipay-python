#!/usr/bin/python
#you can execute this script with the 'python document.py' command
#first you want to map the table that you want to write to ('document' by default) then ensure that you uncomment the lines near the bottom to insert the data into the tbale

import MySQLdb
import time
from time import gmtime, strftime

tableName = 'payer'
# Open database connection
db = MySQLdb.connect("127.0.0.1","root","root","latipay" )

# prepare a cursor object using cursor() method
cursor = db.cursor()
cursor2 = db.cursor()
cursor.execute("SELECT distinct payer_merchant_code FROM latipay.transaction_order;")
#generate a list of all merchants in the DB
merchants = list(cursor.fetchall())

apiQuery = "SELECT code, name, email, tel, target_country_code FROM latipay.merchant_base WHERE code="
infoQuery = "SELECT ip, name, email, tel, target_country_code FROM latipay.merchant_base WHERE code="
#get id_number for each merchant_id
for merchant_code in merchants:
    merchant_code = merchant_code[0]
    merchant_code = "'" + merchant_code + "'"
    cursor2.execute(apiQuery + merchant_code +';')
    trans_info = cursor2.fetchone()
    if trans_info is not None and trans_info[1] != '' and trans_info[1] is not None:

        payer_id = trans_info[0]
        name = trans_info[1]
        email = trans_info[2]
        phone = trans_info[3]
        country = trans_info[4]

        


    # insert1 = "INSERT INTO" + tableName + "(payer_id, name, email, phone, country) VALUES ("
    # insert2 = payer_id + ',' + name + ',' + email + ',' + phone + ',' + country");"
    # insertQuery = insert1 + insert2
    # insertCursor = db.cursor()
    # insertCursor.execute(insertQuery)
    # db.commit()

print ('success!')

# disconnect from server
db.close()
