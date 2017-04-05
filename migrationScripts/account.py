#!/usr/bin/python
#you can execute this script with the 'python document.py' command
#first you want to map the table that you want to write to ('document' by default) then ensure that you uncomment the lines near the bottom to insert the data into the tbale

import MySQLdb
import time
from time import gmtime, strftime

tableName = 'account'
# Open database connection
db = MySQLdb.connect("127.0.0.1","root","root","latipay" )

# prepare a cursor object using cursor() method
cursor = db.cursor()
cursor2 = db.cursor()
cursor3 = db.cursor()
cursor4 = db.cursor()
cursor.execute("SELECT code FROM latipay.merchant_base;")
#generate a list of all merchants in the DB
merchants = list(cursor.fetchall())

baseQuery = "SELECT first_name, last_name, address, nationality, birthday FROM latipay.merchant_info WHERE code="
baseQuery2 = "SELECT email, tel FROM latipay.merchant_base WHERE code="
baseQuery3 = "SELECT currency FROM latipay.bank_account WHERE merchant_code="

#get id_number for each merchant_id
for merchant_code in merchants:
    merchant_code = merchant_code[0]
    merchant_code = "'" + merchant_code + "'"
    cursor2.execute(baseQuery + merchant_code +';')
    cursor3.execute(baseQuery2 + merchant_code +';')
    cursor4.execute(baseQuery3 + merchant_code +';')

    account_info = cursor2.fetchone()
    if account_info is not None and account_info[1] != '' and account_info[1] is not None:
        contact_name = account_info[0] + ' ' + account_info[1]
        contact_address = account_info[2]
        contact_nationality = account_info[3]
        if account_info[4] is not None and account_info[4] != '':
            birthday = time.strftime("%Y:%m:%d")
            print birthday
        else: birthday = account_info[4]
    
    contact_info = cursor3.fetchone()
    if contact_info is not None and contact_info[0] != '' and contact_info[0] is not None:
        contact_phone = contact_info[1]
        contact_email = contact_info[0]

    currency = cursor4.fetchone()
    if currency is not None and currency[0] != '' and currency[0] is not None:
        currency = currency[0]




    # insert1 = "INSERT INTO" + tableName + "(contact_name, contact_address, contact_nationality, birthday, contact_phone, contact_email, currency) VALUES ("
    # insert2 = contact_name + ',' + contact_address + ',' + contact_nationality + ',' + birthday + ',' + contact_phone + ',' + contact_email + ',' + currency");"
    # insertQuery = insert1 + insert2
    # insertCursor = db.cursor()
    # insertCursor.execute(insertQuery)
    # db.commit()

print ('success!')

# disconnect from server
db.close()
