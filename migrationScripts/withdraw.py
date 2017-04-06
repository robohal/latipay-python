#!/usr/bin/python
#you can execute this script with the 'python document.py' command
#first you want to map the table that you want to write to ('document' by default) then ensure that you uncomment the lines near the bottom to insert the data into the tbale

import MySQLdb
import time
from time import gmtime, strftime

tableName = 'withdrawal'
# Open database connection
db = MySQLdb.connect("127.0.0.1","root","root","latipay" )

# prepare a cursor object using cursor() method
cursor = db.cursor()
cursor2 = db.cursor()
cursor.execute("SELECT code FROM latipay.merchant_base;")
#generate a list of all merchants in the DB
merchants = list(cursor.fetchall())

apiQuery = "SELECT settle_order_no, create_date, receive_amount, receive_currency, status, payee_merchant_code FROM latipay.settle_order WHERE payee_merchant_code="
#get id_number for each merchant_id
for merchant_code in merchants:
    merchant_code = merchant_code[0]
    merchant_code = "'" + merchant_code + "'"
    cursor2.execute(apiQuery + merchant_code +';')
    trans_info = cursor2.fetchone()
    if trans_info is not None and trans_info[1] != '' and trans_info[1] is not None:

        transaction_id = trans_info[0]
        amount = trans_info[2]
        # payment_method = trans_info[2]
        if trans_info[1] is not None and trans_info[1] != '':
            create_date = time.strftime("%Y:%m:%d")
        else: create_date = trans_info[1]
        # amount = trans_info[]
        organisation_id = trans_info[4]
        status = trans_info[5]
        currency = trans_info[3]
        # type = 'invoice'


    # insert1 = "INSERT INTO" + tableName + "(settle_order_no, amount, create_date, organisation_id, currency) VALUES ("
    # insert2 = settle_order_no + ',' + amount + ',' + create_date + ',' + organisation_id + ',' + currency");"
    # insertQuery = insert1 + insert2
    # insertCursor = db.cursor()
    # insertCursor.execute(insertQuery)
    # db.commit()

print ('success!')

# disconnect from server
db.close()
