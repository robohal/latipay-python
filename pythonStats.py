#!/usr/bin/python

import MySQLdb
from datetime import datetime
# from elasticsearch import Elasticsearch
from scipy import stats
import numpy as np
import pylab
# import collections
# es = Elasticsearch()

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
    # generate dict with sums of transactions grouped by type.  variable output is called 'typePairs'
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
    # generate dict with sums of transactions grouped by industry.  variable output is called 'industryPairs'
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
    # calculate transaction sums for each merchant and bucket by month.  insert sums into stats table.
    query = "SELECT merchant_code FROM test5;"
    cursor9.execute(query)
    merchant_codes = cursor9.fetchall()
    row = 0
    for merchant_code in merchant_codes:
        print 'processing row ' + str(row)
        myMonth = 0
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
            # print queryYear
            # print lastYear
            # print queryMonth
            monthIterator = 1
            while queryYear <= lastYear:
                if queryYear != firstYear:
                    queryMonth = 1
                else: 
                    queryMonth = firstMonth
                while ((queryMonth <= lastMonth) or (queryYear < lastYear) and (queryMonth <= 12)):
                    # getting transactions from receiver_order table
                    queryTransactionsByMonth = "SELECT price FROM receiver_order WHERE YEAR(create_date) = " + str(queryYear) + " AND MONTH(create_date) = " + str(queryMonth) + " AND receiver_code=" + merchant_code + ";"
                    cursor12.execute(queryTransactionsByMonth)
                    monthTransactions = cursor12.fetchall()
                    # getting transactions from transaction_order table
                    queryTransactionOrdersByMonth = "SELECT price FROM transaction_order WHERE YEAR(create_date) = " + str(queryYear) + " AND MONTH(create_date) = " + str(queryMonth) + " AND receiver_code=" + merchant_code + ";"
                    cursor14.execute(queryTransactionsByMonth)
                    monthTransaction_orders = cursor14.fetchall()
                    monthSum = 0
                    for transaction in monthTransactions:
                        if transaction[0] == None:
                            transaction = 0
                        else: 
                            transaction = transaction[0]/100
                        monthSum = transaction + monthSum
                    for transactionOrder in monthTransaction_orders:
                        if transactionOrder[0] == None:
                            transactionOrder = 0
                        else: 
                            transactionOrder = transactionOrder[0]/100
                        monthSum = monthSum + transactionOrder
                    # print str(monthSum) + ' ' + str(monthIterator)
                    var_dic[(monthIterator)] = monthSum
                    # print monthSum
                    monthIterator = monthIterator + 1
                    queryMonth = queryMonth + 1
                queryYear = queryYear + 1
            # sort the month buckets dictionary based on month number
            od = sorted(var_dic.items())         
            monthlist = ''      
            datalist = ''   
            count = 0
            for month in od:
                monthlist = monthlist + 'month'+str(month[0]) + ','
                # print str(month[1])
                datalist = datalist + str(month[1]) + ','
                count = count + 1 
            monthlist = monthlist.lstrip()
            monthlist = monthlist.rstrip(',')
           
            # strip a trailing space and comma if there is one
            datalist = datalist.lstrip()
            datalist = datalist.rstrip(',')
            print monthlist
            print datalist
            insert1 = "INSERT INTO stats6 (count, merchant_code, month0," + monthlist +  ") VALUES (" + str(count) + ", "+merchant_code + ", 0," + datalist + ");"
            # print insert1
            cursor13.execute(insert1)
            db.commit()
        # if row > 30:
        #     break
        row = row + 1
    print ('success!')
    # disconnect from server
    db.close()

def calculateOLS():
    # calculate slopes for trendlines of monthly revenue buckets.
    month = 0
    query = "SELECT merchant_code FROM stats5;"
    cursor15.execute(query)
    merchant_codes = cursor15.fetchall()
    row = 0
    # loop through merchant codes from query
    for merchant_code in merchant_codes:
        merchant_code = "'" + merchant_code[0] + "'"
        # get all bucket data for specified merchant code
        query = "SELECT * FROM stats5 WHERE merchant_code=" + merchant_code + ";"
        cursor16.execute(query)
        monthlyBuckets = cursor16.fetchall()
        bucket = []
        # generate list of longs from bucket query
        for sub in monthlyBuckets:
            for oneMonth in sub:
                if type(oneMonth) == long:
                    bucket.append(oneMonth)
        print bucket
        # row += 1
        # if row > 4: 
        return

def sixMonthAvg():
    # calculate slopes for trendlines of monthly revenue buckets.
    month = 0
    query = "SELECT merchant_code FROM stats5;"
    cursor15.execute(query)
    merchant_codes = cursor15.fetchall()
    row = 0
    # loop through merchant codes from query
    totalAvg = []
    for merchant_code in merchant_codes:
        merchant_code = "'" + merchant_code[0] + "'"
        # get all bucket data for specified merchant code
        query = "SELECT * FROM stats6 WHERE merchant_code=" + merchant_code + ";"
        cursor16.execute(query)
        monthlyBuckets = cursor16.fetchall()
        bucket = []
        # generate list of longs from bucket query
        for sub in monthlyBuckets:
            for oneMonth in sub:
                if type(oneMonth) == long:
                    bucket.append(oneMonth)
        avg = 0
        # startPoint = 2
        final = len(bucket)
        monthSum2 = 0 
        row = 0
        for monthSum2 in bucket:
            if row >= 2 and row <= final:
                # print monthSum2
                avg = (monthSum2/100) + avg
            row = row + 1
        avg = avg/(final-2)
        # print avg
        totalAvg.append(avg)
    totalAvg = np.mean(totalAvg)
    # in the first 6 months of signing up to latipay, a merchant gains an average of totalAvg in transactions 
    print totalAvg

def NZDByGatewayByIndustry():
    cursor30 = db.cursor()
    cursor31 = db.cursor()
    cursor32 = db.cursor()
    print 'NZD Gateway'
    queryGateways = "SELECT distinct gateway_type FROM transaction_order;"
    cursor32.execute(queryGateways)
    gateways = cursor32.fetchall()
    
    for gateway in gateways:
        gatewayTotal = 0
        gatewayString = "'" + str(gateway[0]) + "'"
        # print gateway
        query = "SELECT receive_amount FROM transaction_order WHERE gateway_type=" + gatewayString + ";"
        cursor31.execute(query)
        receive_amounts = cursor31.fetchall()
        # print (receive_amounts)
        # print receive_amounts
        gatewayTotal = 0
        
        for healthcare_merchant in healthcare_merchants:
            healthcare_merchant = "'" + healthcare_merchant[0] + "'"
            # print healthcare_merchant
            query = "SELECT receive_amount FROM transaction_order WHERE gateway_type="+gatewayString+" AND merchant_code="+healthcare_merchant+";"
            cursor30.execute(query)
            receive_amounts = cursor30.fetchall()
            
            var_dic = {}
            # print gateways
            counter = 0
            for receive_amount in receive_amounts:
                # print receive_amount
                gatewayTotal = gatewayTotal + receive_amount[0]/100
                counter = counter + 1
            # print gatewayTotal
        if gateway[0] == 1:
            print 'alipay volume: ' + str(gatewayTotal) + '.  alipay counts: ' + str(counter)
        if gateway[0] == 0:
            print 'payease: ' + str(gatewayTotal) + '.  payease counts: ' + str(counter)
        if gateway[0] == 2:
            print 'wechat: ' + str(gatewayTotal) + '.  wechat counts: ' + str(counter)
        if gateway[0] == 4:
            print 'jdpay: ' + str(gatewayTotal) + '.  jdpay counts: ' + str(counter)
            # print gatewayTotal
            # break

def NZDByGateway():
    cursor30 = db.cursor()
    cursor31 = db.cursor()
    cursor32 = db.cursor()
    print 'NZD Gateway'
    queryGateways = "SELECT distinct gateway_type FROM transaction_order;"
    cursor32.execute(queryGateways)
    gateways = cursor32.fetchall()
    
    for gateway in gateways:
        gatewayTotal = 0
        gatewayString = "'" + str(gateway[0]) + "'"
        # print gateway
        query = "SELECT receive_amount FROM transaction_order WHERE gateway_type=" + gatewayString + ";"
        cursor31.execute(query)
        receive_amounts = cursor31.fetchall()
        # print (receive_amounts)
        # print receive_amounts
        gatewayTotal = 0
        
        # print healthcare_merchant
        query = "SELECT receive_amount FROM transaction_order WHERE gateway_type="+gatewayString+";"
        cursor30.execute(query)
        receive_amounts = cursor30.fetchall()
        
        var_dic = {}
        # print gateways
        counter = 0
        for receive_amount in receive_amounts:
            # print receive_amount
            gatewayTotal = gatewayTotal + receive_amount[0]/100
            counter = counter + 1
        # print gatewayTotal
        if gateway[0] == 1:
            print 'alipay volume: ' + str(gatewayTotal) + '.  alipay counts: ' + str(counter)
        if gateway[0] == 0:
            print 'payease: ' + str(gatewayTotal) + '.  payease counts: ' + str(counter)
        if gateway[0] == 2:
            print 'wechat: ' + str(gatewayTotal) + '.  wechat counts: ' + str(counter)
        if gateway[0] == 4:
            print 'jdpay: ' + str(gatewayTotal) + '.  jdpay counts: ' + str(counter)
            # print gatewayTotal
            # break

def merchantListByIndustry():
    # generate dict with sums of transactions grouped by industry.  variable output is called 'industryPairs'
    var_dic = {}
    cursor44 = db.cursor()
    for industry in industries:
        industry = "'" + str(industry[0]) + "'"
        query = "SELECT merchant_code FROM test5 WHERE merchant_industry_text=" + industry + ";"
        cursor44.execute(query)
        merchant_codes_for_industry = cursor44.fetchall()
        # in order to sum the transactions for each type
        # print merchant_codes_for_industry
        var_dic[(industry)] = merchant_codes_for_industry
    # print var_dic["'Accommodation and Food Services'"]
    accomodation_and_food_merchants = var_dic["'Accommodation and Food Services'"]
    global healthcare_merchants
    healthcare_merchants = var_dic["'Healthcare'"]


# NZDByType()
# print ''
# NZDByIndustry()
# print ''
# NZDByCodeByDate()
# print ''
# calculateOLS()
# print ''
# sixMonthAvg()
# print ''
# merchantListByIndustry()
# print ''
# NZDByGatewayByIndustry()
# print ''
# NZDByGateway()



