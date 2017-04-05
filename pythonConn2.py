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
cursor3 = db.cursor()
cursor4 = db.cursor()
cursor5 = db.cursor()
cursor6 = db.cursor()
cursor7 = db.cursor()
cursor8 = db.cursor()
cursor9 = db.cursor()
cursor10 = db.cursor()
cursor11 = db.cursor()
cursor12 = db.cursor()
cursor13 = db.cursor()
cursor14 = db.cursor()
cursor15 = db.cursor()
cursor16 = db.cursor()
cursor17 = db.cursor()
cursor18 = db.cursor()
cursor19 = db.cursor()
cursor20 = db.cursor()
cursor21 = db.cursor()
cursor22 = db.cursor()
cursor23 = db.cursor()
cursor24 = db.cursor()
cursor25 = db.cursor()
cursor26 = db.cursor()
cursor27 = db.cursor()
cursor28 = db.cursor()
cursor29 = db.cursor()
cursor46 = db.cursor()
cursor47 = db.cursor()
cursor48 = db.cursor()

#create table with name = tableName
# cursor28.execute("CREATE TABLE test5 (merchant_code VARCHAR(255), merchant_name VARCHAR(255), merchant_address_street VARCHAR(255), merchant_address_city VARCHAR(255), merchant_address_province VARCHAR(255), merchant_address_country VARCHAR(255), merchant_email VARCHAR(255), merchant_phone VARCHAR(255), merchant_comment VARCHAR(255), merchant_type_text VARCHAR(255), merchant_industry_text VARCHAR(255), merchant_physical_presence_text VARCHAR(255), merchant_residence_text VARCHAR(255), merchant_incorporation_text VARCHAR(255), merchant_trade_country_text VARCHAR(255), merchant_product_text VARCHAR(255));")
# execute SQL query using execute() method.
cursor.execute("SELECT code FROM latipay.merchant_base;")


#generate a list of all merchants in the DB
global merchants
merchants = list(cursor.fetchall())
baseQuery = "SELECT name FROM latipay.merchant_base WHERE code="
baseQuery3 = "SELECT email FROM latipay.merchant_base WHERE code="
baseQuery4 = "SELECT tel FROM latipay.merchant_base WHERE code="
baseQuery5 = "SELECT city FROM latipay.address WHERE merchant_code="
baseQuery6 = "SELECT remark FROM latipay.address WHERE merchant_code="
baseQuery7 = "SELECT province FROM latipay.address WHERE merchant_code="
baseQuery8 = "SELECT country FROM latipay.address WHERE merchant_code="
baseQuery9 = "SELECT content FROM latipay.business_name WHERE merchant_code="

#merchant_info table adds
# baseQuery13 = "SELECT occupation FROM latipay.merchant_info WHERE code="
# baseQuery14 = "SELECT tel FROM latipay.merchant_info WHERE code="
# baseQuery15 = "SELECT city FROM latipay.merchant_info WHERE merchant_code="
# baseQuery16 = "SELECT remark FROM latipay.merchant_info WHERE merchant_code="
# baseQuery17 = "SELECT province FROM latipay.merchant_info WHERE merchant_code="
# baseQuery18 = "SELECT country FROM latipay.merchant_info WHERE merchant_code="
# baseQuery19 = "SELECT content FROM latipay.merchant_info WHERE merchant_code="

#Merchant type lookups
baseQuery10 = "SELECT option_id FROM latipay.merchant_question_option WHERE question_id=1 AND merchant_code="
baseQuery11 = "SELECT option_text FROM latipay.question_option WHERE id="

#Merchant industry lookups
baseQuery12 = "SELECT option_id FROM latipay.merchant_question_option WHERE question_id=2 AND merchant_code="
baseQuery13 = "SELECT option_text FROM latipay.question_option WHERE id="

#Merchant physical presence lookups
baseQuery14 = "SELECT option_id FROM latipay.merchant_question_option WHERE question_id=8 AND merchant_code="
baseQuery15 = "SELECT option_text FROM latipay.question_option WHERE id="

#Merchant residence country lookups
baseQuery16 = "SELECT option_id FROM latipay.merchant_question_option WHERE question_id=10 AND merchant_code="
baseQuery17 = "SELECT option_text FROM latipay.question_option WHERE id="

#Merchant incorporation country lookups
baseQuery18 = "SELECT option_id FROM latipay.merchant_question_option WHERE question_id=11 AND merchant_code="
baseQuery19 = "SELECT option_text FROM latipay.question_option WHERE id="

#Merchant country of trade lookups
baseQuery20 = "SELECT option_id FROM latipay.merchant_question_option WHERE question_id=12 AND merchant_code="
baseQuery21 = "SELECT option_text FROM latipay.question_option WHERE id="

#Merchant customer count lookups
baseQuery22 = "SELECT option_id FROM latipay.merchant_question_option WHERE question_id=7 AND merchant_code="
baseQuery23 = "SELECT option_text FROM latipay.question_option WHERE id="

#Merchant product lookups
baseQuery24 = "SELECT option_id FROM latipay.merchant_question_option WHERE question_id=14 AND merchant_code="
baseQuery25 = "SELECT option_text FROM latipay.question_option WHERE id="

#Merchant expected value / annum
baseQuery26 = "SELECT option_id FROM latipay.merchant_question_option WHERE question_id=4 AND merchant_code="
baseQuery27 = "SELECT option_text FROM latipay.question_option WHERE id="

#Merchant expected value / annum
baseQuery46 = "SELECT option_id FROM latipay.merchant_question_option WHERE question_id=6 AND merchant_code="
baseQuery47 = "SELECT option_id FROM latipay.merchant_question_option WHERE question_id=8 AND merchant_code="
baseQuery48 = "SELECT option_text FROM latipay.question_option WHERE id="


print('Finished collecting merchant ids...')

#get merchant_name for each merchant_id
row = 0
for merchant_code in merchants:
    # print ('processing: ' + str(row))
    merchant_code = str(merchant_code)
    merchant_code = merchant_code.split('(')
    merchant_code = str(merchant_code[1])
    merchant_code = merchant_code.split(',')
    merchant_code = str(merchant_code[0])

    #query variables from legacy DB and convert to strings
    cursor2.execute(baseQuery + merchant_code +';')
    merchant_name = cursor2.fetchone()
    if merchant_name != None:
        merchant_name = "'" + merchant_name[0] + "'"
    else: merchant_name = "null"
    cursor3.execute(baseQuery3 + merchant_code +';')
    merchant_email = cursor3.fetchone()
    if merchant_email != None:
        merchant_email = "'" + merchant_email[0] + "'"
    else: merchant_email = "null"
    cursor4.execute(baseQuery4 + merchant_code +';')
    merchant_phone = cursor4.fetchone()
    if merchant_phone != None:
        merchant_phone = "'" + merchant_phone[0] + "'"
    else: merchant_phone = "null"
    cursor5.execute(baseQuery5 + merchant_code +';')
    merchant_address_city = cursor5.fetchone()
    if merchant_address_city != None:
        merchant_address_city = "'" + merchant_address_city[0] + "'"
    else: merchant_address_city = "null"
    cursor6.execute(baseQuery6 + merchant_code +';')
    merchant_address_street = cursor6.fetchone()
    if merchant_address_street != None:
        merchant_address_street = "'" + merchant_address_street[0] + "'"
    else: merchant_address_street = "null"
    cursor7.execute(baseQuery7 + merchant_code +';')
    merchant_address_province = cursor7.fetchone()
    if merchant_address_province != None:
        merchant_address_province = "'" + merchant_address_province[0] + "'"
    else: merchant_address_province = "null"
    cursor8.execute(baseQuery8 + merchant_code +';')
    merchant_address_country = cursor8.fetchone()
    if merchant_address_country != None:
        merchant_address_country = "'" + merchant_address_country[0] + "'"
    else: merchant_address_country = "null"
    cursor9.execute(baseQuery9 + merchant_code +';')
    merchant_comment = cursor9.fetchone()
    if merchant_comment != None:
        merchant_comment = "'" + merchant_comment[0] + "'"
    else: merchant_comment = "null"

    #conditionals for risk assessment enumerated data
    cursor10.execute(baseQuery10 + merchant_code +';')
    merchant_type_code = cursor10.fetchone()
    if merchant_type_code != None:
        merchant_type_code = str(merchant_type_code)
        merchant_type_code = merchant_type_code.split('(')
        merchant_type_code = str(merchant_type_code[1])
        merchant_type_code = merchant_type_code.split(',')
        merchant_type_code = str(merchant_type_code[0])
        merchant_type_code = merchant_type_code.strip("L")
        merchant_type_code.split("L")
        cursor11.execute(baseQuery11 + merchant_type_code +';')
        merchant_type_text = cursor11.fetchone()
        merchant_type_text = "'" + merchant_type_text[0] + "'"
    else: merchant_type_text = "null"

    cursor12.execute(baseQuery12 + merchant_code +';')
    merchant_industry_code = cursor12.fetchone()
    if merchant_industry_code != None:
        merchant_industry_code = str(merchant_industry_code)
        merchant_industry_code = merchant_industry_code.split('(')
        merchant_industry_code = str(merchant_industry_code[1])
        merchant_industry_code = merchant_industry_code.split(',')
        merchant_industry_code = str(merchant_industry_code[0])
        merchant_industry_code = merchant_industry_code.strip("L")
        merchant_industry_code.split("L")
        cursor13.execute(baseQuery13 + merchant_industry_code +';')
        merchant_industry_text = cursor13.fetchone()
        merchant_industry_text = "'" + merchant_industry_text[0] + "'"
    else: merchant_industry_text = "null"

    cursor14.execute(baseQuery14 + merchant_code +';')
    merchant_physical_presence_code = cursor14.fetchone()
    if merchant_physical_presence_code != None:
        merchant_physical_presence_code = str(merchant_physical_presence_code)
        merchant_physical_presence_code = merchant_physical_presence_code.split('(')
        merchant_physical_presence_code = str(merchant_physical_presence_code[1])
        merchant_physical_presence_code = merchant_physical_presence_code.split(',')
        merchant_physical_presence_code = str(merchant_physical_presence_code[0])
        merchant_physical_presence_code = merchant_physical_presence_code.strip("L")
        merchant_physical_presence_code.split("L")
        cursor15.execute(baseQuery15 + merchant_physical_presence_code +';')
        merchant_physical_presence_text = cursor15.fetchone()
        merchant_physical_presence_text = "'" + merchant_physical_presence_text[0] + "'"
    else: merchant_physical_presence_text = "null"

    cursor16.execute(baseQuery16 + merchant_code +';')
    merchant_residence_code = cursor16.fetchone()
    if merchant_residence_code != None:
        merchant_residence_code = str(merchant_residence_code)
        merchant_residence_code = merchant_residence_code.split('(')
        merchant_residence_code = str(merchant_residence_code[1])
        merchant_residence_code = merchant_residence_code.split(',')
        merchant_residence_code = str(merchant_residence_code[0])
        merchant_residence_code = merchant_residence_code.strip("L")
        merchant_residence_code.split("L")
        cursor17.execute(baseQuery17 + merchant_residence_code +';')
        merchant_residence_text = cursor17.fetchone()
        merchant_residence_text = "'" + merchant_residence_text[0] + "'"
    else: merchant_residence_text = "null"

    cursor18.execute(baseQuery18 + merchant_code +';')
    merchant_incorporation_code = cursor18.fetchone()
    if merchant_incorporation_code != None:
        merchant_incorporation_code = str(merchant_incorporation_code)
        merchant_incorporation_code = merchant_incorporation_code.split('(')
        merchant_incorporation_code = str(merchant_incorporation_code[1])
        merchant_incorporation_code = merchant_incorporation_code.split(',')
        merchant_incorporation_code = str(merchant_incorporation_code[0])
        merchant_incorporation_code = merchant_incorporation_code.strip("L")
        merchant_incorporation_code.split("L")
        cursor19.execute(baseQuery19 + merchant_incorporation_code +';')
        merchant_incorporation_text = cursor19.fetchone()
        merchant_incorporation_text = "'" + merchant_incorporation_text[0] + "'"
    else: merchant_incorporation_text = "null"

    cursor20.execute(baseQuery20 + merchant_code +';')
    merchant_trade_country_code = cursor20.fetchone()
    if merchant_trade_country_code != None:
        merchant_trade_country_code = str(merchant_trade_country_code)
        merchant_trade_country_code = merchant_trade_country_code.split('(')
        merchant_trade_country_code = str(merchant_trade_country_code[1])
        merchant_trade_country_code = merchant_trade_country_code.split(',')
        merchant_trade_country_code = str(merchant_trade_country_code[0])
        merchant_trade_country_code = merchant_trade_country_code.strip("L")
        merchant_trade_country_code.split("L")
        cursor21.execute(baseQuery21 + merchant_trade_country_code +';')
        merchant_trade_country_text = cursor21.fetchone()
        merchant_trade_country_text = "'" + merchant_trade_country_text[0] + "'"
    else: merchant_trade_country_text = "null"

    # merchant_customer_count_code
    # cursor22.execute(baseQuery22 + merchant_code +';')
    # merchant_customer_count_code = cursor22.fetchone()
    # if merchant_customer_count_code != None:
    #     merchant_customer_count_code = str(merchant_customer_count_code)
    #     merchant_customer_count_code = merchant_customer_count_code.split('(')
    #     merchant_customer_count_code = str(merchant_customer_count_code[1])
    #     merchant_customer_count_code = merchant_customer_count_code.split(',')
    #     merchant_customer_count_code = str(merchant_customer_count_code[0])
    #     merchant_customer_count_code = merchant_customer_count_code.strip("L")
    #     merchant_customer_count_code.split("L")
    #     cursor23.execute(baseQuery23 + merchant_customer_count_code +';')
    #     merchant_customer_count_text = cursor23.fetchone()
    #     merchant_customer_count_text = str(merchant_customer_count_text)
    #     merchant_customer_count_text = merchant_customer_count_text.split('(')
    #     merchant_customer_count_text = str(merchant_customer_count_text[1])
    #     merchant_customer_count_text = merchant_customer_count_text.split(',')
    #     merchant_customer_count_text = str(merchant_customer_count_text[0])
    # else: merchant_customer_count_text = ''
    # print merchant_customer_count_text

    cursor24.execute(baseQuery24 + merchant_code +';')
    merchant_product_code = cursor24.fetchone()
    if merchant_product_code != None:
        merchant_product_code = str(merchant_product_code)
        merchant_product_code = merchant_product_code.split('(')
        merchant_product_code = str(merchant_product_code[1])
        merchant_product_code = merchant_product_code.split(',')
        merchant_product_code = str(merchant_product_code[0])
        merchant_product_code = merchant_product_code.strip("L")
        merchant_product_code.split("L")
        cursor25.execute(baseQuery25 + merchant_product_code +';')
        merchant_product_text = cursor25.fetchone()
        merchant_product_text = str(merchant_product_text[0])
        merchant_product_text = "'" + merchant_product_text + "'"
    else: merchant_product_text = "null"

    # expected transaction value per annum
    cursor46.execute(baseQuery46 + merchant_code +';')
    cursor47.execute(baseQuery47 + merchant_code +';')
    merchant_expected_trans_value_code = cursor46.fetchone()
    merchant_expected_trans_value_text = "null"
    if merchant_expected_trans_value_code == None:
        merchant_expected_trans_value_code = cursor47.fetchone()
    if merchant_expected_trans_value_code != None:
        merchant_expected_trans_value_code = "'" + str(merchant_expected_trans_value_code[0]) + "'"
        cursor48.execute(baseQuery48 + merchant_expected_trans_value_code +';')
        merchant_expected_trans_value_text = cursor48.fetchone()
        print "expected value is: " + str(merchant_expected_trans_value_text[0])
        merchant_expected_trans_value_text = str(merchant_expected_trans_value_text[0])
        merchant_expected_trans_value_text = "'" + merchant_expected_trans_value_text + "'"
    else: merchant_expected_trans_value_text = "null"

    #tracks progress
    row = row + 1

    insert1 = "INSERT INTO test7 (merchant_code, merchant_name, merchant_address_street, merchant_address_city, merchant_address_province, merchant_address_country, merchant_email, merchant_phone, merchant_comment, merchant_type_text, merchant_industry_text, merchant_physical_presence_text, merchant_residence_text, merchant_incorporation_text, merchant_trade_country_text, merchant_product_text, merchant_expected_trans_value_text) VALUES ("
    insert2 = merchant_code + ',' + merchant_name + ',' + merchant_address_street + ',' + merchant_address_city + ',' + merchant_address_province + ',' + merchant_address_country + ',' + merchant_email + ',' + merchant_phone + ',' + merchant_comment + ',' + merchant_type_text + ',' + merchant_industry_text + ',' + merchant_physical_presence_text + ',' + merchant_residence_text + ',' + merchant_incorporation_text + ',' + merchant_trade_country_text + ',' + merchant_product_text + ',' + merchant_expected_trans_value_text + ");"
    insertQuery = insert1 + insert2
    
    cursor29.execute(insertQuery)
    db.commit()

    # if row > 20:
    #     break

print ('success!')

# disconnect from server
db.close()
