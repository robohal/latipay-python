#!/usr/bin/python
#you can execute this script with the 'python document.py' command
#first you want to map the table that you want to write to ('document' by default) then ensure that you uncomment the lines near the bottom to insert the data into the tbale

import MySQLdb
import time
from time import gmtime, strftime

tableName = 'bank_account'
# Open database connection
db = MySQLdb.connect("127.0.0.1","root","root","latipay" )

# prepare a cursor object using cursor() method
cursor = db.cursor()
cursor2 = db.cursor()
cursor.execute("SELECT code FROM latipay.merchant_base;")
#generate a list of all merchants in the DB
merchants = list(cursor.fetchall())

baseQuery = "SELECT account_num, account, register_bank, branch_bank_address, currency, swift_code, merchant_code FROM latipay.bank_account WHERE merchant_code="
#get id_number for each merchant_id
for merchant_code in merchants:
    merchant_code = merchant_code[0]
    merchant_code = "'" + merchant_code + "'"
    cursor2.execute(baseQuery + merchant_code +';')

    bank_account_info = cursor2.fetchone()
    if bank_account_info is not None and bank_account_info[1] != '' and bank_account_info[1] is not None:
        print bank_account_info
        bank_account_number = bank_account_info[0]
        account_name = bank_account_info[1]
        bank_name = bank_account_info[2]
        bank_address = bank_account_info[3]
        settlement_currency = bank_account_info[4]
        swift_code = bank_account_info[5]
        merchant_code = bank_account_info[6]



    # insert1 = "INSERT INTO" + tableName + "(bank_account_number, account_name, bank_name, bank_address, settlement_currency, swift_code, merchant_code) VALUES ("
    # insert2 = bank_account_number + ',' + account_name + ',' + account_name + ',' + bank_name + ',' + bank_address + ',' + settlement_currency + ',' + swift_code + ',' + merchant_code");"
    # insertQuery = insert1 + insert2
    # insertCursor = db.cursor()
    # insertCursor.execute(insertQuery)
    # db.commit()

print ('success!')

# disconnect from server
db.close()
