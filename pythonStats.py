#!/usr/bin/python

import MySQLdb
from datetime import datetime
from elasticsearch import Elasticsearch
from scipy import stats
import numpy as np
import pylab
es = Elasticsearch()

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
    typePairs = {}
    # loop through merchant types and return merchants that fit each type
    for type in types:
        type = "'" + str(type[0]) + "'"
        query = "SELECT merchant_code FROM test5 WHERE merchant_type_text=" + type + ";"
        cursor3.execute(query)
        merchant_codes_for_type = cursor3.fetchall()
        # in order to sum the transactions for each type
        revenueSumByType = 0
        for merchant_code_for_type in merchant_codes_for_type:
            merchant_code_for_type = "'" + str(merchant_code_for_type[0]) + "'"
            queryAPI = "SELECT receive_amount FROM transaction_order WHERE merchant_code=" + merchant_code_for_type + ";"
            queryReceiver = "SELECT price FROM receiver_order WHERE receiver_code=" + merchant_code_for_type + ";"
            # API transaction data
            cursor7.execute(queryAPI)
            revenueAPI = cursor7.fetchone()
            if revenueAPI != None:
                revenueSumByType = revenueSumByType + revenueAPI[0]
            # Receiver / invoice data
            cursor5.execute(queryReceiver)
            revenueReceiver = cursor3.fetchone()
            if revenueReceiver != None:
                revenueSumByType = revenueSumByType + revenueReceiver[0]
        typePairs[type] = revenueSumByType
    print typePairs

def NZDByIndustry():
    industryPairs = {}
    for industry in industries:
        industry = "'" + str(industry[0]) + "'"
        query = "SELECT merchant_code FROM test5 WHERE merchant_industry_text=" + industry + ";"
        cursor4.execute(query)
        merchant_codes_for_industry = cursor4.fetchall()
        # in order to sum the transactions for each type
        revenueSumByIndustry = 0
        for merchant_code_for_industry in merchant_codes_for_industry:
            merchant_code_for_industry = "'" + str(merchant_code_for_industry[0]) + "'"
            # print merchant_code_for_type
            queryAPI = "SELECT receive_amount FROM transaction_order WHERE merchant_code=" + merchant_code_for_industry + ";"
            queryReceiver = "SELECT price FROM receiver_order WHERE receiver_code=" + merchant_code_for_industry + ";"
            cursor6.execute(queryAPI)
            revenueAPI = cursor6.fetchone()
            if revenueAPI != None:
                revenueSumByIndustry = revenueSumByIndustry + revenueAPI[0]
            #industry receiver/invoice sumcursor4.execute(query)
            cursor8.execute(queryAPI)
            revenueReceiver= cursor8.fetchone()
            if revenueReceiver != None:
                revenueSumByIndustry = revenueSumByIndustry + revenueReceiver[0]
        # print industry +': ' + str(revenueSumByIndustry)
        industryPairs[industry] = revenueSumByIndustry
    print industryPairs

def NZDByCodeByDate():
    fundsArray = {'merchants':[]}
    # generate arrays for each merchant for times
    # times should start at the create date for the account
    # iterator should sum transactions by day.  Transaction sum for a given day should be the Y axis.  Day is X axis.
    query = "SELECT merchant_code FROM test5;"
    cursor9.execute(query)
    merchant_codes = cursor9.fetchall()
    row = 0
    for merchant_code in merchant_codes:
        var_dic = {}
        merchant_code = "'" + merchant_code[0] + "'"
        merchant_code = str(merchant_code)
        # print merchant_code
        query = "SELECT price, create_date FROM receiver_order WHERE price IS NOT NULL AND receiver_code=" + merchant_code + ";"
        queryDates = "SELECT create_date FROM receiver_order WHERE price IS NOT NULL AND receiver_code=" + merchant_code + ";"
        cursor10.execute(query)
        received_funds = cursor10.fetchall()
        cursor11.execute(queryDates)
        dateList = cursor11.fetchall()
        # print dateList
        if received_funds:
            #getting first and last dates of transactions for a given merchant
            firstDate = min(dateList)
            lastDate = max(dateList)
            firstDate = firstDate[0].date()
            lastDate = lastDate[0].date()
            firstMonth = firstDate.month
            firstYear = firstDate.year
            lastMonth = lastDate.month
            lastYear = lastDate.year

            queryYear = firstYear
            queryMonth = firstMonth
            print queryYear
            print lastYear
            print queryMonth
            monthIterator = 1
            while queryYear <= lastYear:
                if queryYear != firstYear:
                    queryMonth = 1
                else: queryMonth = firstMonth
                print 'bucketing by year'
                while ((queryMonth <= lastMonth) or (queryYear < lastYear) and (queryMonth <= 12) and monthIterator <= 24):
                    for monthIterator in range(1,25):
                        
                        # var_dic["month%s"% str(monthIterator)] = 0
                        
                        queryTransactionsByMonth = "SELECT price FROM receiver_order WHERE YEAR(create_date) = " + str(queryYear) + " AND MONTH(create_date) = " + str(queryMonth) + " AND receiver_code=" + merchant_code + ";"
                        cursor12.execute(queryTransactionsByMonth)
                        monthTransactions = cursor12.fetchall()
                        monthSum = 0
                        for transaction in monthTransactions:
                            if transaction[0] != None:
                                monthSum = transaction[0] + monthSum
                        # month[monthIterator] = 0
                        print 'fingers crossed...'
                        print month[monthIterator]
                        # print month2
                        insertMonth = 'month' + str(monthIterator) + ' ' + str(monthSum)
                        var_dic["month%s"% str(monthIterator)] = monthSum
                        monthIterator += 1
                        queryMonth = queryMonth + 1
                    queryYear = queryYear + 1
        print var_dic                       
        if row == 0:
            break
        row = row + 1
        

# NZDByType()
# print ''
# NZDByIndustry()
print ''
NZDByCodeByDate()



print ('success!')
# disconnect from server
db.close()
