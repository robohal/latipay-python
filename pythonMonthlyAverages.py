#!/usr/bin/python

import MySQLdb
from datetime import datetime
# from elasticsearch import Elasticsearch
from scipy import stats
import numpy as np
import pylab
import time
from time import gmtime, strftime
# import collections
# es = Elasticsearch()

db = MySQLdb.connect("127.0.0.1","root","root","latipay" )

def monthlySumsReceivers():
    # calculate transaction sums for each merchant and bucket by month.  insert sums into stats table.
    # get the merchant code list from test7
    merchantCursor = db.cursor()
    merchantQuery = "SELECT merchant_code FROM test7;"
    merchantCursor.execute(merchantQuery)
    merchant_codes = merchantCursor.fetchall()
    row = 0
    for merchant_code in merchant_codes:
        monthlist = ''
        datalist = ''
        monthIterator = 1
        volumeCursor = db.cursor()
        var_dic = {}
        merchant_code = "'" + merchant_code[0] + "'"
        merchant_code = str(merchant_code)
        global merchant_code
        # query the receiver_order table for all transactions 
        lastActiveReceiver()
        volumeQuery = "SELECT SUM(price) FROM receiver_order WHERE receiver_code= " + merchant_code + "GROUP BY YEAR(create_date), MONTH(create_date);"
        volumeCursor.execute(volumeQuery)
        received_funds = volumeCursor.fetchall()

        for monthSum in received_funds:
            var_dic[(monthIterator)] = monthSum
            monthIterator = monthIterator +1
        if var_dic != {}:
            # print var_dic.keys()[0]
            activeMonths = len(var_dic)
            # print activeMonths
        # print var_dic
        od = sorted(var_dic.items())  
        for month in od:
            # print type(month[1])
            if month[1] != None:
                monthSum = month[1]
                monthSum = monthSum[0]
            else:
                monthSum = 0
            if monthSum != 0 and monthSum != None:
                monthSum = monthSum/100
            if monthSum == None:
                monthSum = 0
            # print monthSum
            # print month[0]
            monthlist = monthlist + 'month'+str(month[0]) + ','
            datalist = datalist + str(monthSum) + ','
        monthlist = monthlist.lstrip()
        monthlist = monthlist.rstrip(',')
        # strip a trailing space and comma if there is one
        datalist = datalist.lstrip()
        datalist = datalist.rstrip(',')
        dataLength = len(datalist)
        
        # if datalist:
        # print datalist
        if dataLength > 1:
            # print dataLength
            insert1 = "INSERT INTO monthlyInvoicingStats2 (lastActive, activeMonths, merchant_code, month0," + monthlist +  ") VALUES (" + str(lastActiveDate) + ", " + str(activeMonths) + ", "+merchant_code + ", 0," + datalist + ");"
            print insert1
            cursor13 = db.cursor()
            cursor13.execute(insert1)
            db.commit()
        

    # print ('success!')
    # # disconnect from server
    db.close()

def monthlySumsTransactions():
    # calculate transaction sums for each merchant and bucket by month.  insert sums into stats table.
    # get the merchant code list from test7
    global merchant_code
    merchantCursor = db.cursor()
    merchantQuery = "SELECT merchant_code FROM test7;"
    merchantCursor.execute(merchantQuery)
    merchant_codes = merchantCursor.fetchall()
    row = 0
    for merchant_code in merchant_codes:
        
        monthlist = ''
        datalist = ''
        monthIterator = 1
        volumeCursor = db.cursor()
        var_dic = {}
        merchant_code = "'" + merchant_code[0] + "'"
        merchant_code = str(merchant_code)
        lastActive()
        # query the receiver_order table for all transactions 
        volumeQuery = "SELECT SUM(receive_amount) FROM transaction_order WHERE merchant_code= " + merchant_code + "GROUP BY YEAR(create_date), MONTH(create_date);"
        volumeCursor.execute(volumeQuery)
        received_funds = volumeCursor.fetchall()

        for monthSum in received_funds:
            var_dic[(monthIterator)] = monthSum
            monthIterator = monthIterator +1
        if var_dic != {}:
            # print var_dic.keys()[0]
            activeMonths = len(var_dic)
            # print activeMonths
        # print var_dic
        od = sorted(var_dic.items())  
        for month in od:
            # print type(month[1])
            if month[1] != None:
                monthSum = month[1]
                monthSum = monthSum[0]
            else:
                monthSum = 0
            if monthSum != 0 and monthSum != None:
                monthSum = monthSum/100
            if monthSum == None:
                monthSum = 0
            # print monthSum
            # print month[0]
            monthlist = monthlist + 'month'+str(month[0]) + ','
            datalist = datalist + str(monthSum) + ','
        monthlist = monthlist.lstrip()
        monthlist = monthlist.rstrip(',')
        # strip a trailing space and comma if there is one
        datalist = datalist.lstrip()
        datalist = datalist.rstrip(',')
        dataLength = len(datalist)
        
        # if datalist:
        # print datalist
        if dataLength > 1:
            
            insert1 = "INSERT INTO monthlyTransactionStats2 (lastActive, activeMonths, merchant_code, month0," + monthlist +  ") VALUES (" + str(lastActiveDate) + ", " + str(activeMonths) + ", "+merchant_code + ", 0," + datalist + ");"
            print insert1
            cursor13 = db.cursor()
            cursor13.execute(insert1)
            db.commit()
        

    # print ('success!')
    # # disconnect from server
    db.close()

def monthSumsTransactions():
    # calculate transaction sums for each merchant and bucket by month.  insert sums into stats table.
    # get the merchant code list from test7
    global merchant_code
    startDate = '2017-01-01'
    endDate = time.strftime("%Y:%m:%d")
    dateCursor = db.cursor()
    merchantCursor = db.cursor()
    merchantQuery = "SELECT merchant_code FROM test7;"
    merchantCursor.execute(merchantQuery)
    merchant_codes = merchantCursor.fetchall()
    row = 0
    for merchant_code in merchant_codes:
        
        monthlist = ''
        datalist = ''
        monthIterator = 1
        volumeCursor = db.cursor()
        var_dic = {}
        merchant_code = "'" + merchant_code[0] + "'"
        merchant_code = str(merchant_code)
        lastActive()
        # query the receiver_order table for all transactions 
        dateQuery = "SELECT YEAR(create_date), MONTH(create_date) FROM transaction_order GROUP BY YEAR(create_date), MONTH(create_date);"
        volumeQuery = "SELECT SUM(receive_amount/100), YEAR(create_date), MONTH(create_date) FROM transaction_order WHERE merchant_code= " + merchant_code + "GROUP BY YEAR(create_date), MONTH(create_date);"
        volumeCursor.execute(volumeQuery)
        received_funds = volumeCursor.fetchall()
        dateCursor.execute(dateQuery)
        dateRange = dateCursor.fetchall()
        # print received_funds
        

        for monthSum in received_funds:
            date_data = str(monthSum[1]) + '-' + str(monthSum[2]) + '-' + '15'
            print date
        for month in dateRange:
            date_range = str(monthSum[1]) + '-' + str(monthSum[2]) + '-' + '15'
            # print date
        break
        
        for 

        for month in od:
            # print type(month[1])
            if month[1] != None:
                monthSum = month[1]
                monthSum = monthSum[0]
            else:
                monthSum = 0
            if monthSum != 0 and monthSum != None:
                monthSum = monthSum/100
            if monthSum == None:
                monthSum = 0
            # print monthSum
            # print month[0]
            monthlist = monthlist + 'month'+str(month[0]) + ','
            datalist = datalist + str(monthSum) + ','
        monthlist = monthlist.lstrip()
        monthlist = monthlist.rstrip(',')
        # strip a trailing space and comma if there is one
        datalist = datalist.lstrip()
        datalist = datalist.rstrip(',')
        dataLength = len(datalist)
        
        # if datalist:
        # print datalist
        if dataLength > 1:
            
            insert1 = "INSERT INTO monthlyTransactionStats2 (lastActive, activeMonths, merchant_code, month0," + monthlist +  ") VALUES (" + str(lastActiveDate) + ", " + str(activeMonths) + ", "+merchant_code + ", 0," + datalist + ");"
            print insert1
            cursor13 = db.cursor()
            cursor13.execute(insert1)
            db.commit()
        

    # print ('success!')
    # # disconnect from server
    db.close()

def lastActive():
 # get date last active for merchant
    # print merchxant_code
    cursorLastActive = db.cursor()
    lastActiveQuery = "select create_date from transaction_order where merchant_code = " +  merchant_code + "order by create_date desc LIMIT 1;"
    cursorLastActive.execute(lastActiveQuery)
    global lastActiveDate
    lastActiveDate = cursorLastActive.fetchone()
    if lastActiveDate: 
        # print lastActiveDate
        # print lastActive
        lastActiveDate = "'" + lastActiveDate[0].strftime('%Y-%m-%d') + "'"
        print lastActiveDate

def lastActiveReceiver():
 # get date last active for merchant
    # print merchxant_code
    cursorLastActive = db.cursor()
    lastActiveQuery = "select create_date from receiver_order where receiver_code = " +  merchant_code + "order by create_date desc LIMIT 1;"
    cursorLastActive.execute(lastActiveQuery)
    global lastActiveDate
    lastActiveDate = cursorLastActive.fetchone()
    if lastActiveDate: 
        # print lastActiveDate
        # print lastActive
        lastActiveDate = "'" + lastActiveDate[0].strftime('%Y-%m-%d') + "'"
        print lastActiveDate



# monthlySumsTransactions()
monthSumsTransactions()
# monthlySumsReceivers()