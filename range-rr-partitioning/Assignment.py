#!usr/bin/python

import os
import psycopg2 as ps
import re
import string
import sys
from datetime import datetime

import Interface

# ratingstablename global
master_table = 'ratings'

def loadratings(ratingstablename, ratingsfilepath, openconnection):
  start = datetime.now()
  
  # Variables
  table = ratingstablename
  global master_table
  master_table = table

  ratingsfilepath = os.path.join(os.getcwd(), ratingsfilepath)

  cur = openconnection.cursor()

  check_sql = 'SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ' + \
              '\'public\' AND table_name = \'' + ratingstablename + '\''
              
  cur.execute(check_sql)
  
  if cur.fetchone()[0] > 0:
    print 'INFO: Data already loaded!'
    return
  
  create_sql = 'CREATE TABLE ' + table + '( ' + \
               'userid INT, ' + \
               'null1 TEXT, ' + \
               'movieid INT, ' + \
               'null2 TEXT, ' + \
               'rating DECIMAL(2, 1), ' + \
               'null3 TEXT, ' + \
               'timestamp TEXT ' + \
               ')' 
  cur.execute(create_sql)

  cur.copy_from(open(ratingsfilepath, 'r'), ratingstablename, sep=':', size=10000)
    
  alter_sql = 'ALTER TABLE IF EXISTS ' + table + \
              ' DROP COLUMN null1, DROP COLUMN null2, DROP COLUMN null3, DROP COLUMN timestamp'
  cur.execute(alter_sql)
  
  openconnection.commit()
  
  end = datetime.now()
  return end - start

def rangepartition(ratingstablename, numberofpartitions, openconnection):
  start = datetime.now()

  if not validate(partitions=numberofpartitions):
    return

  cur = openconnection.cursor()

  check_sql = 'SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ' + \
              '\'public\' AND table_name LIKE \'rat\\_%\''
  cur.execute(check_sql)
  
  if cur.fetchone()[0] > 0:
    deletepartitionsandexit(openconnection)
  
  # Variables
  table = ratingstablename
  global master_table
  master_table = table
  prefix = 'rat_rnge'
  diff = 5.0 / numberofpartitions

  # Create bak table
  bak_sql = 'CREATE TABLE IF NOT EXISTS bak_rat AS SELECT * FROM ' + table
  cur.execute(bak_sql)

  # Empty ratings
  trunc_sql = 'TRUNCATE TABLE ' + table
  cur.execute(trunc_sql)

  # Partitioning
  part_sql = 'CREATE TABLE ' + prefix + '0 (' + \
             'CHECK (rating >= %s AND rating <= %s)' + \
             ') INHERITS (' + table + ')'
  cur.execute(part_sql, (0, diff))
  insert_sql = 'INSERT INTO ' + prefix + '0 SELECT * FROM bak_rat ' + \
               'WHERE rating >= 0 AND rating <= ' + str(diff)
  cur.execute(insert_sql)
  index = diff
  for i in range(1, numberofpartitions):
    child = prefix + str(i)
    part_sql = 'CREATE TABLE ' + child + ' (' + \
               'CHECK (rating > %s AND rating <= %s)' + \
               ') INHERITS (' + table + ')'
    cur.execute(part_sql, (index, index + diff))
    
    insert_sql = 'INSERT INTO ' + child + ' SELECT * FROM bak_rat ' + \
                 'WHERE rating > ' + str(index) + ' AND rating <= ' + str(index + diff)
    cur.execute(insert_sql)
    index += diff


  # range_insert() function for rangeinsert()
  trigfunc_sql = 'CREATE OR REPLACE FUNCTION range_insert() ' + \
                 'RETURNS TRIGGER AS $$ ' + \
                 'BEGIN '
  
  trigfunc_sql += 'IF (NEW.rating >= 0 AND NEW.rating <= ' + \
                    str(diff) + ') THEN INSERT INTO ' + prefix + '0 VALUES (NEW.*); '
  
  index = diff
  for i in range(1, numberofpartitions):
    child = prefix + str(i)
    trigfunc_sql += 'ELSIF (NEW.rating > ' + str(index) + ' AND NEW.rating <= ' + \
                    str(index + diff) + ') THEN INSERT INTO ' + child + ' VALUES (NEW.*); '
    index += diff

  trigfunc_sql += 'END IF; ' + \
                  'RETURN NULL; ' + \
                  'END; ' + \
                  '$$ ' + \
                  'LANGUAGE plpgsql;'
  cur.execute(trigfunc_sql)

  # Trigger for ratings table insert
  trigger_sql = 'CREATE TRIGGER range_trigger ' + \
                'BEFORE INSERT ON ' + table + ' ' + \
                'FOR EACH ROW EXECUTE PROCEDURE range_insert()'
  cur.execute(trigger_sql)
  
  openconnection.commit()
  
  end = datetime.now()
  return end - start

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
  start = datetime.now() 
  
  global master_table
  master_table = ratingstablename
  
  if not validate(rating=rating):
    return
  
  cur = openconnection.cursor()
  try:
    insert_sql = 'INSERT INTO ' + ratingstablename + ' (userid, movieid, rating) ' + \
                 'VALUES (%s, %s, %s)'
    cur.execute(insert_sql, (userid, itemid, rating))
  except ps.Error as e:
    print 'ERROR:', e.diag.message_primary
    sys.exit()
  openconnection.commit()
  end = datetime.now()
  return end - start

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
  start = datetime.now()

  if not validate(partitions=numberofpartitions):
    return

  cur = openconnection.cursor()

  check_sql = 'SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ' + \
              '\'public\' AND table_name LIKE \'rat\\_%\''
  cur.execute(check_sql)
  if cur.fetchone()[0] > 0:
    deletepartitionsandexit(openconnection)
  
  # Variables
  table = ratingstablename
  global master_table
  master_table = table
  prefix = 'rat_roro'
  index = 0

  meta_sql = 'CREATE TABLE meta_rat ( ' + \
             'next INT ' + \
             ') '
  cur.execute(meta_sql)

  metai_sql = 'INSERT INTO meta_rat (next) VALUES (0)'
  cur.execute(metai_sql)

  # Copy data to bak table and truncate ratings
  copy_sql = 'CREATE TABLE IF NOT EXISTS bak_rat AS ' + \
             'SELECT * FROM ' + table
  cur.execute(copy_sql)
    
  trunc_sql = 'TRUNCATE TABLE ' + table
  cur.execute(trunc_sql)
  for i in range(numberofpartitions):
    child = prefix + str(i)
    part_sql = 'CREATE TABLE ' + child + '( ) INHERITS (' + table + ')'
    cur.execute(part_sql)
  
    # Copy data into partitions
    # Trigger not used for set up due to performance overhead
    fill_sql = 'WITH ratingrows AS (' + \
               'SELECT *, ROW_NUMBER() OVER() AS rownum FROM bak_rat ' + \
               ') INSERT INTO ' + child + '(userid, movieid, rating) ' + \
               'SELECT userid, movieid, rating FROM ratingrows ' + \
               'WHERE (rownum - 1) % ' + str(numberofpartitions) + ' = ' + str(i)
    cur.execute(fill_sql)
  
  count_sql = 'SELECT COUNT(*) FROM ' + table
  cur.execute(count_sql)

  rownum = cur.fetchone()
  
  # robin_insert() function for roundrobininsert()
  trigfunc_sql = 'CREATE OR REPLACE FUNCTION robin_insert() ' + \
                 'RETURNS TRIGGER AS $$ ' + \
                 'DECLARE rownum INT; tname TEXT; '  + \
                 'BEGIN ' + \
                 'tname := \'' + prefix + str(rownum[0] % numberofpartitions) + '\';' + \
                 'EXECUTE \'INSERT INTO \' || tname || \' SELECT * FROM (SELECT $1.*) AS t\' ' + \
                 'USING NEW; ' + \
                 'RETURN NULL; ' + \
                 'END; ' + \
                 '$$ ' + \
                 'LANGUAGE plpgsql;'
  cur.execute(trigfunc_sql)
  
  # Trigger for ratings table insert
  trigger_sql = 'CREATE TRIGGER robin_trigger ' + \
                'BEFORE INSERT ON ' + table + ' ' + \
                'FOR EACH ROW EXECUTE PROCEDURE robin_insert()'
  cur.execute(trigger_sql)
  
  openconnection.commit()
  end = datetime.now()
  return end - start

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
  start = datetime.now()
  
  global master_table
  master_table = ratingstablename
  
  if not validate(rating=rating):
    return
  
  cur = openconnection.cursor()
  try:
    insert_sql = 'INSERT INTO ' + ratingstablename + ' (userid, movieid, rating) ' + \
                 'VALUES (%s, %s, %s)'
    cur.execute(insert_sql, (userid, itemid, rating))
  except ps.Error as e:
    print 'ERROR:', e.diag.message_primary
    sys.exit()
  openconnection.commit()
  end = datetime.now()
  return end - start

def deletepartitionsandexit(openconnection):
  start = datetime.now()
  cur = openconnection.cursor()
  
  # Remove ratings and partitions
  check_sql = 'SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ' + \
              '\'public\' AND table_name LIKE \'rat\\_%\''
  cur.execute(check_sql)
  if cur.fetchone()[0] > 0:
    dat_sql = 'DROP TABLE IF EXISTS ' + master_table + ' CASCADE'
    cur.execute(dat_sql)
  
  # Remove meta table
  meta_sql = 'DROP TABLE IF EXISTS meta_rat'
  cur.execute(meta_sql)
  
  # Remove triggers
  trig_sql = 'DROP TRIGGER IF EXISTS range_trigger ON ' + master_table
  cur.execute(trig_sql)
  trig_sql = 'DROP TRIGGER IF EXISTS robin_trigger ON ' + master_table
  cur.execute(trig_sql)
  
  # Restore ratings
  sql = 'ALTER TABLE IF EXISTS bak_rat RENAME TO ' + master_table
  cur.execute(sql)

  openconnection.commit()
  end = datetime.now()
  return end - start

def validate(rating=None, partitions=None):
  if rating is not None:
    if isinstance(rating, (int, float)) and rating >= 0 and rating <= 5:
      return True
    print 'ERROR: Invalid value of rating. Not a primitive integer (0 <= x <= 5).'
  if partitions is not None:
    if isinstance(partitions, int) and partitions > 0:
      return True
    print 'ERROR: Invalid value of number of partitions. Not a primitive integer (x > 0).'
  return False

if __name__ == '__main__':
  print 'CSE512 Assignment 1 Script started...'
  conn = Interface.getopenconnection()

  datfilepath = 'test_data.dat'

  t = loadratings('ratings', datfilepath, conn)
  print 'loadratings() complete. Time: ' + str(t)

  #t = rangepartition('ratings', 7, conn)
  #print 'rangepartition() complete. Time: ' + str(t)

  #t = rangeinsert('ratings', 80000, 70000, 2.5, conn)
  #print 'rangeinsert() complete. Time: ' + str(t)
  
  #t = roundrobinpartition('ratings', 21, conn)
  #print 'roundrobinpartition() complete. Time: ' + str(t)

  #t = roundrobininsert('ratings', 80001, 70001, 2.5, conn)
  #print 'roundrobininsert() complete. Time: ' + str(t)
  
  #t = deletepartitionsandexit(conn)
  #print 'deletepartitionsandexit() complete. Time: ' + str(t)
  
  print 'Fin.'
