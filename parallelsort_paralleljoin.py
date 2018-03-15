#!/usr/bin/python2.7
#
# Assignment3 Interface
#

# Pratik Suryawanshi
# psuryawa@asu.edu
# ASU ID : 1213231238

import psycopg2
import os
import sys
import threading

THREAD_COUNT = 5
RANGE_PARTITION_PREFIX = "rangeparition"
JOIN_RANGE_PARTITION_PREFIX = "joinrangepartition"
TABLE1_RANGE_PARTITION_PREFIX = "table1_rangeparition"
TABLE2_RANGE_PARTITION_PREFIX = "table2_rangeparition"

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'


##########################################################################################################

# helper functions
def copySchema(source, destination, cursor):
    dbquery = "CREATE TABLE {0} AS SELECT * FROM {1} WHERE 1=2".format(destination, source)
    cursor.execute(dbquery);


def parallelsorting(InputTable, rangetable, SortingColumnName, minBound, maxBound, openconnection):
    cur = openconnection.cursor();
    if rangetable == 'rangeparition0':
        dbquery = "INSERT INTO {0} SELECT * FROM {1}  WHERE {2} >= {3}  AND {2} <= {4} ORDER BY {2} ASC".format(rangetable, InputTable, SortingColumnName, minBound, maxBound)
    else:
        dbquery = "INSERT INTO {0}  SELECT * FROM {1}  WHERE {2}  > {3}  AND {2}  <= {4} ORDER BY {2} ASC".format(rangetable, InputTable, SortingColumnName, minBound, maxBound)
    cur.execute(dbquery)


def rangepartitioning(inputtable, tablejoincolumn, pInterval, min, max, prefixname, cur):
    for i in range(THREAD_COUNT):
        rangePartitionedTable = prefixname + repr(i)

        if i == 0:
            minLimit = min
            maxLimit = minLimit + pInterval
            dbquery = "CREATE TABLE {0} AS  SELECT * FROM {1}  WHERE {2} >= {3} AND {2} <= {4};".format(rangePartitionedTable, inputtable, tablejoincolumn, minLimit, maxLimit)
        else:
            minLimit = maxLimit
            maxLimit = minLimit + pInterval
            dbquery = "CREATE TABLE {0} AS  SELECT * FROM {1}  WHERE {2} > {3} AND {2} <= {4};".format(rangePartitionedTable, inputtable, tablejoincolumn, minLimit, maxLimit)

        cur.execute(dbquery)


def createRangePartitionForJoin(table1, table2, outputtable, cur):
    dbquery = "CREATE TABLE {0} AS SELECT * FROM {1},{2} WHERE 1=2".format(outputtable, table1, table2)
    cur.execute(dbquery);


def paralleljoinhelper(table1, table2, joinColumn1, joinColumn2, outputtable, openconnection):
    cur = openconnection.cursor()
    dbquery = "insert into {0} select * from {1} INNER JOIN {2} ON {1}.{3} = {2}.{4}".format(outputtable, table1, table2, joinColumn1, joinColumn2)
    cur.execute(dbquery)


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    cur = openconnection.cursor()

    # find min in given input table for SortingColumn
    dbquery = "select  min({0}) from {1} ".format(SortingColumnName, InputTable)
    cur.execute(dbquery)
    minBound = cur.fetchone()[0]

    # Find max in given input table for SortingColumn
    dbquery = "select  max({0}) from {1} ".format(SortingColumnName, InputTable)
    cur.execute(dbquery)
    maxBound = cur.fetchone()[0]

    pInterval = abs(maxBound - minBound) / float(THREAD_COUNT)


    # to create range partitioning so that each thread can work on seperate thread.
    for i in range(THREAD_COUNT):
        outputrangetable = RANGE_PARTITION_PREFIX + repr(i);
        copySchema(InputTable, outputrangetable, cur)

    # Creating output table
    copySchema(InputTable, OutputTable, cur)
    # get threadpool for parallel sorting
    threadPool = range(THREAD_COUNT);

    for i in range(THREAD_COUNT):
        if i == 0:
            begin = minBound
            end = minBound + pInterval
        else:
            begin = end
            end = end + pInterval
        rangePartitionedTableName = RANGE_PARTITION_PREFIX + repr(i)
        threadPool[i] = threading.Thread(target=parallelsorting, args=(InputTable, rangePartitionedTableName, SortingColumnName, begin, end, openconnection))
        threadPool[i].start()

    # wait till all threads are done.
    for i in range(THREAD_COUNT):
        threadPool[i].join()

    # insert data into final output table
    for i in range(THREAD_COUNT):
        rangePartitionedTableName = RANGE_PARTITION_PREFIX + repr(i)
        dbquery = "INSERT INTO {0} SELECT * FROM {1}".format(OutputTable, rangePartitionedTableName)
        cur.execute(dbquery)

    # remove temp rangepartitioned tables.
    for i in range(THREAD_COUNT):
        rangePartitionedTableName = RANGE_PARTITION_PREFIX + repr(i)
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(rangePartitionedTableName))

    openconnection.commit()


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()

    # STEP-1 RangePartition
    # get min1 and max1 from table 1
    dbquery = "select  min({0}) from {1} ".format(Table1JoinColumn, InputTable1)
    cur.execute(dbquery)
    min1 = cur.fetchone()[0]
    dbquery = "select  max({0}) from {1} ".format(Table1JoinColumn, InputTable1)
    cur.execute(dbquery)
    max1 = cur.fetchone()[0]

    # get min2 and max2 of input table 2
    dbquery = "select  min({0}) from {1} ".format(Table2JoinColumn, InputTable2)
    cur.execute(dbquery)
    min2 = cur.fetchone()[0]
    dbquery = "select  max({0}) from {1} ".format(Table2JoinColumn, InputTable2)
    cur.execute(dbquery)
    max2 = cur.fetchone()[0]

    minInMins = min(min1, min2)
    maxInMaxes = max(max1, max2)

    pInterval = abs(maxInMaxes - minInMins) / float(THREAD_COUNT)
    rangepartitioning(InputTable1, Table1JoinColumn, pInterval, minInMins, maxInMaxes, TABLE1_RANGE_PARTITION_PREFIX, cur)
    rangepartitioning(InputTable2, Table2JoinColumn, pInterval, minInMins, maxInMaxes, TABLE2_RANGE_PARTITION_PREFIX, cur)

    # Create temp tables for range joining assigned to each thread

    for i in range(THREAD_COUNT):
        outputtable = JOIN_RANGE_PARTITION_PREFIX + repr(i)
        createRangePartitionForJoin(InputTable1, InputTable2, outputtable, cur)

    # thread pool for performing parallel sort
    threadPool = range(THREAD_COUNT);

    for i in range(THREAD_COUNT):
        inputtable1 = TABLE1_RANGE_PARTITION_PREFIX + repr(i)
        inputtable2 = TABLE2_RANGE_PARTITION_PREFIX + repr(i)
        outputtable = JOIN_RANGE_PARTITION_PREFIX + repr(i)
        threadPool[i] = threading.Thread(target=paralleljoinhelper, args=(inputtable1, inputtable2, Table1JoinColumn, Table2JoinColumn, outputtable, openconnection))
        threadPool[i].start()

    # wait tll all threads are done
    for i in range(THREAD_COUNT):
        threadPool[i].join()

    # create final output table
    createRangePartitionForJoin(InputTable1, InputTable2, OutputTable, cur)

    # insert all results in the output table
    for i in range(THREAD_COUNT):
        tablename = JOIN_RANGE_PARTITION_PREFIX + repr(i)
        dbquery = "INSERT INTO {0} SELECT * FROM {1}".format(OutputTable, tablename)
        cur.execute(dbquery)

    # Delete all intermediate partitions
    for i in range(THREAD_COUNT):
        table1 = TABLE1_RANGE_PARTITION_PREFIX + repr(i)
        table2 = TABLE2_RANGE_PARTITION_PREFIX + repr(i)
        table3 = JOIN_RANGE_PARTITION_PREFIX + repr(i)
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(table1))
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(table2))
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(table3))

    openconnection.commit()



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
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
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" % (ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d` + ",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


if __name__ == '__main__':
    try:
    # Creating Database ddsassignment3
        print "Creating Database named as ddsassignment3"
        createDB();

    # Getting connection to the database
        print "Getting connection from the ddsassignment3 database"
        con = getOpenConnection();

    # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

    # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE,
                 'parallelJoinOutputTable', con);

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
