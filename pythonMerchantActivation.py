#!/usr/bin/python

import MySQLdb
from datetime import datetime
import numpy as np
import csv

db = MySQLdb.connect("127.0.0.1","root","root","latipay" )

def merchantActivations():
    # parameters for merchants to query
    table = 'test7'
    filter = 'merchant_expected_trans_value_text'
    filterValue = '>$10M'
    cursor = db.cursor()
    cursor2 = db.cursor()
    cursor3 = db.cursor()
    
    # query = "SELECT merchant_code FROM latipay." + table + " WHERE merchant_type_text IS NOT NULL AND " + filter + "=" + "'" + filterValue + "'" + ";"
    query = "SELECT merchant_code FROM latipay." + table + " WHERE merchant_type_text IS NOT NULL;"
    cursor.execute(query)
    merchants_enriched = list(cursor.fetchall())
    # print merchants_enriched
    # query the transactions for each merchant code from the above query
    for merchant_code in merchants_enriched:
        # print merchant_code
        merchant_code = "'" + merchant_code[0] + "'"
        query3 = "SELECT SUM(receive_amount) FROM latipay.transaction_order WHERE merchant_code=" + merchant_code + ";"
        activeQuery = "SELECT count FROM latipay.stats6 WHERE merchant_code=" + merchant_code + ";"
        # print query
        cursor2.execute(query3)
        cursor3.execute(activeQuery)

        trans_sum = cursor2.fetchone()
        print trans_sum
        active_months = cursor3.fetchone()
        # print cursor3.fetchall()
        # print active_months
        if active_months == None: 
            active_months = 1
        # active_months = active_months[0] - 1
        else: 
            active_months = active_months[0]
        active_months = active_months - 1
        # print active_months

        if trans_sum[0] != None:
            trans_sum = trans_sum[0]
            trans_sum = trans_sum/100
            # print trans_sum
        else:
            trans_sum = 0
            # print 0

        if active_months != 0:
            monthlyAverage = trans_sum/active_months

        # if monthlyAverage != 0:
            # print monthlyAverage


def merchantAvgsAPI():
    cursor = db.cursor()
    query = 'SELECT (SUM(receive_amount)/100) as sum, merchant_code FROM transaction_order GROUP BY merchant_code ORDER BY sum;'
    cursor.execute(query)
    merchantSums = list(cursor.fetchall())
    # print merchantSums
    data_trans = [['merchant_code', 'active_months', 'monthly_average_transaction', 'projectedValue', 'lastActive']]
    for merchant in merchantSums:
        cursor2 = db.cursor()
        # print merchant[1]
        activeQuery = "SELECT count FROM latipay.stats6 WHERE merchant_code=" + "'" + merchant[1] + "'" + ";"
        cursor2.execute(activeQuery)
        merchantActives = cursor2.fetchone()

        cursor3 = db.cursor()
        projectionsQuery = "SELECT merchant_expected_trans_value_text FROM latipay.test7 WHERE merchant_code=" + "'" + merchant[1] + "'" + ";"
        cursor3.execute(projectionsQuery)
        projectedValue = cursor3.fetchone()
        if projectedValue != None:
            projectedValue = str(projectedValue[0])
        else: 
            projectedValue = "null"

         # get date last active for merchant
        cursorLastActive = db.cursor()
        lastActiveQuery = "select create_date from transaction_order where merchant_code = " + "'" + merchant[1] + "'" + "order by create_date desc LIMIT 1;"
        cursorLastActive.execute(lastActiveQuery)
        lastActiveDate = cursorLastActive.fetchone()
        lastActiveDate = lastActiveDate[0]
        print lastActiveDate

        if merchantActives != None:
            
            print "Merchant: " + merchant[1] + ";  Active: " + str(merchantActives[0]) + "; " + "Monthly Avg Trans: " + str(merchant[0]/merchantActives[0]) + "; Projected Value: " + projectedValue
            trans_file = open("trans.txt", "a")
            trans_file.write("Merchant: " + merchant[1] + ";  Active: " + str(merchantActives[0]) + "; " + "Monthly Avg Trans: " + str(merchant[0]/merchantActives[0]) + "; Projected Value: " + projectedValue + "\n")
            trans_file.close()
            data_trans.append([merchant[1], merchantActives[0], merchant[0]/merchantActives[0], projectedValue, lastActiveDate])
    print data_trans
    with open('api.csv', 'w') as fp:
            a = csv.writer(fp, delimiter=',')
            data = data_trans
            a.writerows(data_trans)

def merchantAvgsInvoice():
    cursor = db.cursor()
    query = 'SELECT (SUM(price_of_tags_amount)/100) as sum, receiver_code FROM receiver_order GROUP BY receiver_code ORDER BY sum;'
    cursor.execute(query)
    merchantSums = list(cursor.fetchall())
    data_pay = [['merchant_code', 'active_months', 'monthly_average_transaction', 'projectedValue', 'lastActive']]
    # print merchantSums
    for merchant in merchantSums:
        cursor2 = db.cursor()
        # print merchant[1]
        activeQuery = "SELECT count FROM latipay.stats6 WHERE merchant_code=" + "'" + merchant[1] + "'" + ";"
        cursor2.execute(activeQuery)
        merchantActives = cursor2.fetchone()

        cursor3 = db.cursor()
        projectionsQuery = "SELECT merchant_expected_trans_value_text FROM latipay.test7 WHERE merchant_code=" + "'" + merchant[1] + "'" + ";"
        cursor3.execute(projectionsQuery)
        projectedValue = cursor3.fetchone()
        if projectedValue != None:
            projectedValue = str(projectedValue[0])
        else: 
            projectedValue = "null"

        # get date last active for merchant
        cursorLastActive = db.cursor()
        lastActiveQuery = "select done_date from receiver_order where receiver_code = " + "'" + merchant[1] + "'" + "order by done_date desc LIMIT 1;"
        cursorLastActive.execute(lastActiveQuery)
        lastActiveDate = cursorLastActive.fetchone()
        lastActiveDate = lastActiveDate[0]
        print lastActiveDate
        
        if merchantActives != None:
            
            print "Merchant: " + merchant[1] + ";  Active: " + str(merchantActives[0]) + "; " + "Monthly Avg Trans: " + str(merchant[0]/merchantActives[0]) + "; Projected Value: " + projectedValue
            payment_file = open("invoices.txt", "a")
            payment_file.write("Merchant: " + merchant[1] + ";  Active: " + str(merchantActives[0]) + "; " + "Monthly Avg Trans: " + str(merchant[0]/merchantActives[0]) + "; Projected Value: " + projectedValue + "\n")
            payment_file.close()
            data_pay.append([merchant[1], merchantActives[0], merchant[0]/merchantActives[0], projectedValue, lastActiveDate])

    print data_pay
    with open('invoices.csv', 'w') as fp:
            a = csv.writer(fp, delimiter=',')
            data = data_pay
            a.writerows(data_pay)

# merchantActivations()

merchantAvgsAPI()
# merchantAvgsInvoice()

row = 0
# for merchant_code in merchants_enriched: