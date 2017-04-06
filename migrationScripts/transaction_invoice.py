#!/usr/bin/python
#you can execute this script with the 'python document.py' command
#first you want to map the table that you want to write to ('document' by default) then ensure that you uncomment the lines near the bottom to insert the data into the tbale

import MySQLdb
import time
from time import gmtime, strftime

tableName = 'transaction'
# Open database connection
db = MySQLdb.connect("127.0.0.1","root","root","latipay" )

# prepare a cursor object using cursor() method
cursor = db.cursor()
cursor2 = db.cursor()
cursor.execute("SELECT code FROM latipay.merchant_base;")
#generate a list of all merchants in the DB
merchants = list(cursor.fetchall())

apiQuery = "SELECT receive_no, create_date, price_of_tags_amount, price_of_tags_currency, product_name, payee, receiver_code FROM latipay.receiver_order WHERE receiver_code="
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

        product_type = trans_info[4]
        payer_id = trans_info[5]

        organisation_id = trans_info[6]
        # user_id = trans_info[7]
        currency = trans_info[3]
        type = 'invoice'


    # insert1 = "INSERT INTO" + tableName + "(transaction_id, amount, payment_method, create_date, product_type, payer_id, organisation_id, user_id, currency, type) VALUES ("
    # insert2 = transaction_id + ',' + amount + ',' + payment_method + ',' + create_date + ',' + product_type + ',' + payer_id  + ',' + organisation_id  + ',' + user_id + ',' + currency + ',' + type");"
    # insertQuery = insert1 + insert2
    # insertCursor = db.cursor()
    # insertCursor.execute(insertQuery)
    # db.commit()

print ('success!')

# disconnect from server
db.close()
