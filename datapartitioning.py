#!/usr/bin/python2.7
#
# Interface for the assignement
#


#"Pratik Suryawanshi"
#1213231238
#"psuryawa@asu.edu"

import psycopg2
import tempfile
import csv

DATABASE_NAME = 'dds_assgn1'
UserId_Col = 'userId'
MovieId_Col = 'movieId'
Rating_Col = 'rating'
Range_Table_Pre = 'range_part'
Robin_Table_Pre = 'rrobin_part'
Ratings_Table = 'ratings'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def createTable(name, cur):
    try:
        dbquery = "CREATE TABLE {0} ( {1} integer, {2} integer,{3} real);".format(name,
                                                                                UserId_Col, MovieId_Col,
                                                                                Rating_Col)
        cur.execute(dbquery)
    except Exception as e:
        print("Failed to create table exception is : ", e)

def getTableCount(cur, tableprefix):
    dbquery = "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND                                                                         table_name LIKE '{0}%';".format(
        tableprefix)
    cur.execute(dbquery)
    pCount = int(cur.fetchone()[0])
    return pCount


def getCountRowsInTable(cur, tablename):
    dbquery = "SELECT count(*) FROM {0}".format(tablename)
    cur.execute(dbquery)
    num = int(cur.fetchone()[0])
    return num

def loadratings(ratingstablename, ratingsfilepath, openconnection):


    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()

    command = 'CREATE TABLE IF NOT EXISTS %s ( userid INTEGER,  movieid NUMERIC NOT NULL, rating NUMERIC NOT NULL )' % (
    ratingstablename,)
    cur.execute(command)

    with open(ratingsfilepath, 'r') as f:

        filePointer = tempfile.NamedTemporaryFile()
        rows = csv.reader((line.replace('::', ':') for line in f), delimiter=':')
        for row in rows:
            filePointer.write('\t'.join(row[:-1]) + '\n')
        filePointer.seek(0)
        cur.copy_from(filePointer, Ratings_Table)

    cur.close()
    return


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    # for given range of lower and upper bounds
    rlb = 0.0
    rub = 5.0
    pInterval = abs(rub - rlb) / numberofpartitions

    cur = openconnection.cursor()
    for i in range(0, numberofpartitions):
        partitiontablename = Range_Table_Pre + repr(i)
        createTable(partitiontablename, cur)

        lb = i * pInterval
        ub = lb + pInterval

        if lb == rlb:
            dbquery = " INSERT INTO {0} SELECT * FROM {1} WHERE {2} >= {3} and {2} <= {4}".format(partitiontablename,
                                                                                                ratingstablename,
                                                                                                Rating_Col,
                                                                                                lb,
                                                                                                ub)
        else:
            dbquery = " INSERT INTO {0} SELECT * FROM {1} WHERE {2} > {3} and {2} <= {4}".format(partitiontablename,
                                                                                               ratingstablename,
                                                                                               Rating_Col,
                                                                                               lb,
                                                                                               ub)
        cur.execute(dbquery)
        openconnection.commit()
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    # function for creating round robin partitions
    
    modValue = 0
    
    cur = openconnection.cursor()
    
    for i in range(0, numberofpartitions):
        partitiontablename = Robin_Table_Pre + repr(i)
        createTable(partitiontablename, cur)

        if (i != (numberofpartitions - 1)):
            modValue = i + 1;
        else:
            modValue = 0;

        try:
            dbquery = "INSERT INTO {0} " \
                    "SELECT {1},{2},{3} " \
                    "FROM (SELECT ROW_NUMBER() OVER() as row_number,* FROM {4}) as foo " \
                    "WHERE MOD(row_number,{5}) = cast ('{6}' as bigint) ".format(partitiontablename, UserId_Col,
                                                                                 MovieId_Col,
                                                                                 Rating_Col, ratingstablename,
                                                                                 numberofpartitions, modValue)

            cur.execute(dbquery)
            openconnection.commit()
        except Exception as ex:
            print(ex)
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    # Here we are making rows in table same. if all have same rows then add into first else add into talblw with less value

    cur = openconnection.cursor()
    pCount = getTableCount(cur, Robin_Table_Pre)
    pInsert = 0
    previouscount = getCountRowsInTable(cur, Robin_Table_Pre + repr(0))

    for i in range(1, pCount):
        nextcount = getCountRowsInTable(cur, Robin_Table_Pre + repr(i))
        if (nextcount < previouscount):
            pInsert = i
            break

    dbquery = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format(ratingstablename,
                                                             userid, itemid, rating)
    cur.execute(dbquery)

    dbquery = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format(Robin_Table_Pre + repr(pInsert),
                                                             userid, itemid, rating)
    cur.execute(dbquery)
    openconnection.commit()
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    # count of partitions
    pCount = getTableCount(cur, Range_Table_Pre)
    rlb = 0.0
    rub = 5.0
    pInterval = abs(rub - rlb) / pCount

    for i in range(0, pCount):
        lb = i * pInterval
        ub = lb + pInterval

        if lb == rlb:
            if (rating >= lb) and (rating <= ub):
                break
        elif (rating > lb) and (rating <= ub):
            break
    pInsert = i

    dbquery = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format(ratingstablename,
                                                             userid, itemid, rating)
    cur.execute(dbquery)

    dbquery = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format(Range_Table_Pre + repr(pInsert),
                                                             userid, itemid, rating)
    cur.execute(dbquery)
    openconnection.commit()
    pass


def deletepartitionsandexit(openconnection):
    # function to delete partitions.
    cur = openconnection.cursor()
    pCount = getTableCount(cur, Range_Table_Pre)
    for i in range(0, pCount):
        partitionname = Range_Table_Pre + repr(i)
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(partitionname))
        openconnection.commit()

    pCount = getTableCount(cur, Robin_Table_Pre)
    print ('partition count %s', pCount)
    for i in range(0, pCount):
        partitionname = Robin_Table_Pre + repr(i)
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(partitionname))
        openconnection.commit()

    cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(Ratings_Table))
    openconnection.commit()






def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()