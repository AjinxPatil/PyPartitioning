#!/usr/bin/python2.7
#
# rpquery
# Author: Ajinkya Patil
#

import psycopg2
import os
import sys

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()
    sql = """SELECT partitionnum FROM rangeratingsmetadata 
             WHERE minrating <= %s AND maxrating >= %s"""
    cur.execute(sql, (ratingMaxValue, ratingMinValue))
    with open('RangeQueryOut.txt', 'w') as f:
        datset = []
        for row in cur.fetchall():
            part = 'RangeRatingsPart' + str(row[0])
            sql = 'SELECT * FROM ' + part + ' WHERE rating >= %s AND rating <= %s'
            cur.execute(sql, (ratingMinValue, ratingMaxValue))
            records = cur.fetchall()
            writable = [part + ',' + ','.join(map(str, x)) for x in records]
            datset.extend(writable)
        f.write('\n'.join(datset))
    
    sql = "SELECT partitionnum FROM roundrobinratingsmetadata LIMIT 1"
    cur.execute(sql)
    num = cur.fetchone()
    with open('RangeQueryOut.txt', 'a') as f:
        datset=[]
        for i in range(num[0]):
            part = 'RoundRobinRatingsPart' + str(i)
            sql = 'SELECT * FROM ' + part + \
                  ' WHERE rating >= %s AND rating <= %s'
            cur.execute(sql, (ratingMinValue, ratingMaxValue))
            records = cur.fetchall()
            writable = [part + ',' + ','.join(map(str, x)) for x in records]
            datset.extend(writable)
        f.write('\n' + '\n'.join(datset)) 

def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor()
    sql = """SELECT partitionnum FROM rangeratingsmetadata 
             WHERE minrating <= %s AND maxrating >= %s"""
    cur.execute(sql, (ratingValue, ratingValue))
    with open('PointQueryOut.txt', 'w') as f:
        datset = []
        for row in cur.fetchall():
            part = 'RangeRatingsPart' + str(row[0])
            sql = 'SELECT * FROM ' + part + ' WHERE rating = %s'
            cur.execute(sql, (ratingValue,))
            records = cur.fetchall()
            writable = [part + ',' + ','.join(map(str, x)) for x in records]
            datset.extend(writable)
        f.write('\n'.join(datset))
    
    sql = "SELECT partitionnum FROM roundrobinratingsmetadata LIMIT 1"
    cur.execute(sql)
    num = cur.fetchone()
    with open('PointQueryOut.txt', 'a') as f:
        datset=[]
        for i in range(num[0]):
            part = 'RoundRobinRatingsPart' + str(i)
            sql = 'SELECT * FROM ' + part + ' WHERE rating = %s'
            cur.execute(sql, (ratingValue,))
            records = cur.fetchall()
            writable = [part + ',' + ','.join(map(str, x)) for x in records]
            datset.extend(writable)
        f.write('\n' + '\n'.join(datset)) 
