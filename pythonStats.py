#!/usr/bin/python

import MySQLdb
from elasticsearch import Elasticsearch
es = Elasticsearch()

db = MySQLdb.connect("127.0.0.1","root","root","latipay" )

# prepare a cursor object using cursor() method
cursor = db.cursor()
cursor2 = db.cursor()
cursor3 = db.cursor()
cursor4 = db.cursor()
# get the enriched merchant records
cursor.execute("SELECT merchant_code FROM latipay.test5 WHERE merchant_type_text IS NOT NULL;")
#generate a list of all merchants in the DB
merchants_enriched = list(cursor.fetchall())

#generate a list of all the type codes from DB
cursor2.execute("SELECT DISTINCT merchant_type_text FROM latipay.test5;")
types = list(cursor2.fetchall())

cursor3.execute("SELECT DISTINCT merchant_industry_text FROM latipay.test5;")
industries = list(cursor3.fetchall())

row = 0
# for merchant_code in merchants_enriched:

def NZDByType():
    # loop through merchant types and return merchants that fit each type
    for type in types:
        type = "'" + str(type[0]) + "'"
        query = "SELECT merchant_code FROM test5 WHERE merchant_type_text=" + type + ";"
        cursor3.execute(query)
        merchant_codes_for_type = cursor3.fetchall()
        # print merchant_codes_by_type
        # TODO: build a key value pair for types and merchant codes OR loop through each merchant_code list and reference it against the transaction table
        # in order to sum the transactions for each type
        revenueSumByType = 0
        for merchant_code_for_type in merchant_codes_for_type:
            merchant_code_for_type = "'" + str(merchant_code_for_type[0]) + "'"
            # print merchant_code_for_type
            query = "SELECT receive_amount FROM transaction_order WHERE merchant_code=" + merchant_code_for_type + ";"
            cursor3.execute(query)
            revenue = cursor3.fetchone()
            if revenue != None:
                # print int(revenue[0])
                revenueSumByType = revenueSumByType + revenue[0]
        print type +': ' + str(revenueSumByType)

def NZDByIndustry():
    for industry in industries:
        industry = "'" + str(industry[0]) + "'"
        query = "SELECT merchant_code FROM test5 WHERE merchant_industry_text=" + industry + ";"
        cursor4.execute(query)
        merchant_codes_for_industry = cursor4.fetchall()
        # print merchant_codes_by_type
        # TODO: build a key value pair for types and merchant codes OR loop through each merchant_code list and reference it against the transaction table
        # in order to sum the transactions for each type
        revenueSumByIndustry = 0
        for merchant_code_for_industry in merchant_codes_for_industry:
            merchant_code_for_industry = "'" + str(merchant_code_for_industry[0]) + "'"
            # print merchant_code_for_type
            query = "SELECT receive_amount FROM transaction_order WHERE merchant_code=" + merchant_code_for_industry + ";"
            cursor4.execute(query)
            revenue = cursor4.fetchone()
            if revenue != None:
                # print int(revenue[0])
                revenueSumByIndustry = revenueSumByIndustry + revenue[0]
        print industry +': ' + str(revenueSumByIndustry)

    # print ('processing: ' + str(row))
    # merchant_code = "'" + merchant_code[0] + "'"
    
    
    # merchant_name = cursor3.fetchone()
    # if merchant_name != None:
    #     merchant_name = "'" + merchant_name[0] + "'"
    # else: merchant_name = "null"

    #tracks progress
    # row = row + 1

    # insert1 = "INSERT INTO test6 (merchant_code, merchant_name, merchant_address_street, merchant_address_city, merchant_address_province, merchant_address_country, merchant_email, merchant_phone, merchant_comment, merchant_type_text, merchant_industry_text, merchant_physical_presence_text, merchant_residence_text, merchant_incorporation_text, merchant_trade_country_text, merchant_product_text) VALUES ("
    # insert2 = merchant_code + ',' + merchant_name + ',' + merchant_address_street + ',' + merchant_address_city + ',' + merchant_address_province + ',' + merchant_address_country + ',' + merchant_email + ',' + merchant_phone + ',' + merchant_comment + ',' + merchant_type_text + ',' + merchant_industry_text + ',' + merchant_physical_presence_text + ',' + merchant_residence_text + ',' + merchant_incorporation_text + ',' + merchant_trade_country_text + ',' + merchant_product_text + ");"
    # insertQuery = insert1 + insert2
    # cursor29.execute(insertQuery)
    # db.commit()

NZDByType()
print ''
NZDByIndustry()   



print ('success!')
# disconnect from server
db.close()
