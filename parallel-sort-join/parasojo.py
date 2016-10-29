#!/usr/bin/python2.7
#
# parasojo - Parallel Sort and Parallel Join
# Author: Ajinkya Patil

import psycopg2
import os
import sys
import threading

def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    
    # Threads - count, list and thread return value
    THREAD_COUNT = 5
    threads = []
    t_returns = [None for x in range(THREAD_COUNT)]
    
    cur = openconnection.cursor()
    
    # Create OutputTable
    cur.execute('DROP TABLE IF EXISTS ' + OutputTable)
    sql = 'CREATE TABLE ' + OutputTable + ' AS SELECT * FROM ' + InputTable + \
            ' LIMIT 0'
    cur.execute(sql)

    sql = 'SELECT MAX(' +SortingColumnName + '), MIN(' + SortingColumnName + \
            ') FROM ' + InputTable
    cur.execute(sql)
    max_, min_ = cur.fetchone()
    step = (max_ - min_) / float(THREAD_COUNT)
    if step is 0:
        # SortingColumnName column has all same values, no sorting required
        return
    lo = min_

    # Multithreading
    for i in range(THREAD_COUNT):
        hi = lo + step if i < THREAD_COUNT - 1 else lo + 2 * step
        sql = 'SELECT * FROM ' + InputTable + ' WHERE ' + SortingColumnName + \
                ' >= %s AND ' + SortingColumnName + ' < %s ORDER BY ' + \
                SortingColumnName
        t = threading.Thread(target=range_sort, args=(sql, lo, hi,
            openconnection, t_returns,
            i))
        threads.append(t)
        t.start()
        lo = hi
    
    for t in threads:
        t.join()
    
    # Aggregate results
    for result in t_returns:
        if result is None:
            continue
        result_str = [str(x) for x in result]
        values = ','.join(result_str)
        sql = 'INSERT INTO ' + OutputTable + ' VALUES ' + values
        cur.execute(sql)
    openconnection.commit()
    
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    # Threads - count, list and thread return value
    THREAD_COUNT = 1
    threads = []
    t_returns = [None for x in range(THREAD_COUNT)]
    
    cur = openconnection.cursor()

    # Create OutputTable
    cur.execute('DROP TABLE IF EXISTS ' + OutputTable)
    sql1 = 'SELECT * FROM ' + InputTable1
    sql2 = 'SELECT * FROM ' + InputTable2
    sql = 'CREATE TABLE ' + OutputTable + ' AS SELECT * FROM (' + sql2 + \
            ') AS lf INNER JOIN (' + sql1 + ') AS rt ON rt.' + \
            Table1JoinColumn + ' = lf.' + Table2JoinColumn + \
            ' WHERE 1 = 0'
    cur.execute(sql)

    sql = 'SELECT COUNT(*) FROM ' + InputTable1
    cur.execute(sql)
    count = cur.fetchone()[0]

    limit = count / THREAD_COUNT
    offset = 0
    
    #Multithreading
    for i in range(THREAD_COUNT):
        sql_left = 'SELECT * FROM ' + InputTable1 + ' ORDER BY ' + \
                Table1JoinColumn + ' LIMIT %s OFFSET %s'
        sql_limits = 'SELECT MAX(' + Table1JoinColumn + ') AS hi, MIN(' + \
                Table1JoinColumn + ') AS lo FROM (' + sql_left + \
                ') AS lf'
        cur.execute(sql_limits, (limit, offset))
        max_, min_ = cur.fetchone()
        sql_right = 'SELECT * FROM ' + InputTable2 + ' WHERE ' + \
                Table2JoinColumn + ' >= %s AND ' + Table2JoinColumn + \
                ' <= %s'
        if i == THREAD_COUNT - 1:
            limit  = count - i * limit
        t = threading.Thread(target=range_join, args=(sql_left, sql_right,
            Table1JoinColumn, Table2JoinColumn, limit, offset, min_, max_, openconnection, t_returns, i))
        threads.append(t)
        t.start()
        offset = limit
    
    for t in threads:
        t.join()
    
    # Aggregate results
    for result in t_returns:
        if result is None:
            continue
        result_str = [str(x) for x in result]
        values = ','.join(result_str)
        sql = 'INSERT INTO ' + OutputTable + ' VALUES ' + values
        cur.execute(sql)
    openconnection.commit()

def range_join(lq, rq, key1, key2, limit, offset, lo, hi, conn, t_returns, index):
    query = 'SELECT * FROM ('+ rq + ') AS rt INNER JOIN (' + lq + ')' + \
            ' AS lf ON lf.' + key1 + ' = rt.' + key2
    cur = conn.cursor()
    cur.execute(query, (lo, hi, limit, offset))
    t_returns[index] = cur.fetchall()

def range_sort(query, lo, hi, conn, t_returns, index):
    cur = conn.cursor()
    cur.execute(query, (lo, hi))
    t_returns[index] = cur.fetchall()

def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment2
	print "Creating Database named as ddsassignment2"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment2 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
