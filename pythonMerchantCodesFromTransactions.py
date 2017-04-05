#!/usr/bin/python

import MySQLdb

tableName = 'test4'
# Open database connection
db = MySQLdb.connect("127.0.0.1","root","root","latipay" )
print 'start'

def merchantsFromTransactions():
    print 'start22'
    # calculate transaction sums for each merchant and bucket by month.  insert sums into stats table.
    cursorMerchants = db.cursor()
    cursorTrans = db.cursor()
    merchantQuery = "SELECT distinct merchant_code FROM test7;"
    cursorMerchants.execute(merchantQuery)

    transQuery = "SELECT distinct merchant_code FROM transaction_order;"
    cursorTrans.execute(transQuery)

    merchant_codes = cursorMerchants.fetchall()
    trans_merchant_codes = cursorTrans.fetchall()

    trans_merchant_list = []
    row = 0
    newCode = True
    redundant = False
    for merchant_code in merchant_codes:
        # print merchant_code
        for trans_merchant_code in trans_merchant_codes:
            if merchant_code == trans_merchant_code:
                newCode = False
        trans_merchant_list.append(trans_merchant_code)
    trans_merchant_list = set(trans_merchant_list)
    print trans_merchant_list

        # print dateList
    #     if received_funds:
    #         #getting first and last dates of transactions for a given merchant
    #         firstDate = min(dateList)
    #         lastDate = max(dateList)
    #         firstDate = firstDate[0].date()
    #         lastDate = lastDate[0].date()
    #         firstMonth = firstDate.month
    #         firstYear = firstDate.year
    #         lastMonth = lastDate.month
    #         lastYear = lastDate.year

    #         queryYear = firstYear
    #         queryMonth = firstMonth
    #         # print queryYear
    #         # print lastYear
    #         # print queryMonth
    #         monthIterator = 1
    #         while queryYear <= lastYear:
    #             if queryYear != firstYear:
    #                 queryMonth = 1
    #             else: 
    #                 queryMonth = firstMonth
    #             while ((queryMonth <= lastMonth) or (queryYear < lastYear) and (queryMonth <= 12)):
    #                 # getting transactions from receiver_order table
    #                 queryTransactionsByMonth = "SELECT price FROM receiver_order WHERE YEAR(create_date) = " + str(queryYear) + " AND MONTH(create_date) = " + str(queryMonth) + " AND receiver_code=" + merchant_code + ";"
    #                 cursor12.execute(queryTransactionsByMonth)
    #                 monthTransactions = cursor12.fetchall()
    #                 # getting transactions from transaction_order table
    #                 queryTransactionOrdersByMonth = "SELECT price FROM transaction_order WHERE YEAR(create_date) = " + str(queryYear) + " AND MONTH(create_date) = " + str(queryMonth) + " AND receiver_code=" + merchant_code + ";"
    #                 cursor14.execute(queryTransactionsByMonth)
    #                 monthTransaction_orders = cursor14.fetchall()
    #                 monthSum = 0
    #                 for transaction in monthTransactions:
    #                     if transaction[0] == None:
    #                         transaction = 0
    #                     else: 
    #                         transaction = transaction[0]/100
    #                     monthSum = transaction + monthSum
    #                 for transactionOrder in monthTransaction_orders:
    #                     if transactionOrder[0] == None:
    #                         transactionOrder = 0
    #                     else: 
    #                         transactionOrder = transactionOrder[0]/100
    #                     monthSum = monthSum + transactionOrder
    #                 # print str(monthSum) + ' ' + str(monthIterator)
    #                 var_dic[(monthIterator)] = monthSum
    #                 # print monthSum
    #                 monthIterator = monthIterator + 1
    #                 queryMonth = queryMonth + 1
    #             queryYear = queryYear + 1
    #         # sort the month buckets dictionary based on month number
    #         od = sorted(var_dic.items())         
    #         monthlist = ''      
    #         datalist = ''   
    #         count = 0
    #         for month in od:
    #             monthlist = monthlist + 'month'+str(month[0]) + ','
    #             # print str(month[1])
    #             datalist = datalist + str(month[1]) + ','
    #             count = count + 1 
    #         monthlist = monthlist.lstrip()
    #         monthlist = monthlist.rstrip(',')
           
    #         # strip a trailing space and comma if there is one
    #         datalist = datalist.lstrip()
    #         datalist = datalist.rstrip(',')
    #         print monthlist
    #         print datalist
    #         insert1 = "INSERT INTO stats6 (count, merchant_code, month0," + monthlist +  ") VALUES (" + str(count) + ", "+merchant_code + ", 0," + datalist + ");"
    #         # print insert1
    #         cursor13.execute(insert1)
    #         db.commit()
    #     # if row > 30:
    #     #     break
    #     row = row + 1
    # print ('success!')
    # # disconnect from server
    # db.close()

merchantsFromTransactions()