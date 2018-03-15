#!/usr/bin/python2.7
#
# Assignment2 Interface
#

# name: Pratik Suryawanshi
# asu id : 1213231238


import psycopg2
import os
import sys


RANGE_METADATA = 'rangeratingsmetadata'
ROUND_ROBIN_METADATA = 'roundrobinratingsmetadata'
RANGE_PREFIX = 'rangeratingspart'
ROUND_ROBIN_PREFIX = 'roundrobinratingspart'
RANGE_OUTPUT = 'RangeRatingsPart'
ROUND_ROBIN_OUTPUT = 'RoundRobinRatingsPart'
RANGE_OUTPUT_FILE = 'RangeQueryOut.txt'
POINT_OUTPUT_FILE = 'PointQueryOut.txt'


def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    try:
        cursor = openconnection.cursor()
        # get max of minrating from metadata to define range of rating
        dbquery = "select  max(minrating) from {0} where minrating <= {1}".format(RANGE_METADATA, ratingMinValue)
        cursor.execute(dbquery)
        minBoundry = cursor.fetchone()[0]
        # get min of maxrating from metadata to define range of rating
        dbquery = "select  min(maxrating) from {0} where maxrating >= {1}".format(RANGE_METADATA, ratingMaxValue)
        cursor.execute(dbquery)
        maxBoundry = cursor.fetchone()[0]

        dbquery = "select  partitionnum from {0} where maxrating >= {1} and maxrating <= {2}".format(RANGE_METADATA, minBoundry, maxBoundry)
        cursor.execute(dbquery)
        rows = cursor.fetchall()

        # remove file of exists already
        if os.path.exists(RANGE_OUTPUT_FILE):
            os.remove(RANGE_OUTPUT_FILE)

        # write data in file.
        for row in rows:
            partitionName = RANGE_OUTPUT + repr(row[0])
            dbquery = "select * from {0} where rating >= {1} and rating <= {2}".format(partitionName, ratingMinValue,ratingMaxValue)
            cursor.execute(dbquery)
            rows2 = cursor.fetchall()
            with open(RANGE_OUTPUT_FILE, 'a+') as f:
                for x in rows2:
                    f.write("%s," % partitionName)
                    f.write("%s," % str(x[0]))
                    f.write("%s," % str(x[1]))
                    f.write("%s\n" % str(x[2]))


        # to fetch data from round robin paritions for rangequery
        dbquery = "select partitionnum from {0} ".format(ROUND_ROBIN_METADATA)
        cursor.execute(dbquery)
        rrcount = int(cursor.fetchone()[0])

        for i in range(rrcount):
            partitionName = ROUND_ROBIN_OUTPUT + repr(i)
            dbquery = "select * from {0} where rating >= {1} and rating <= {2}".format(partitionName, ratingMinValue, ratingMaxValue)
            cursor.execute(dbquery)
            rows = cursor.fetchall()
            with open(RANGE_OUTPUT_FILE, 'a+') as f:
                for row in rows:
                    f.write("%s," % partitionName)
                    f.write("%s," % str(row[0]))
                    f.write("%s," % str(row[1]))
                    f.write("%s\n" % str(row[2]))


    except Exception as e:
        print("exception has occured while processing range query function : ",e)


def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.

    # Point query for range partition
    # for point query in range partition we can determine which partitions to look for rating data of given rating value.
    try:
        cursor = openconnection.cursor()
        if ratingValue == 0:
            rrpartitioncount = 0
        else:
            dbquery = "select partitionnum from {0} where minrating < {1} and maxrating >= {1}".format(RANGE_METADATA, ratingValue)
            cursor.execute(dbquery)
            rrpartitioncount = cursor.fetchone()[0]

        partitionName = RANGE_OUTPUT + repr(rrpartitioncount)
        dbquery = "select * from {0} where rating = {1} ".format(partitionName, ratingValue)
        cursor.execute(dbquery)
        rows = cursor.fetchall()

        if os.path.exists(POINT_OUTPUT_FILE):
            os.remove(POINT_OUTPUT_FILE)

        with open(POINT_OUTPUT_FILE, 'a+') as f:
            for row in rows:
                f.write("%s," % partitionName)
                f.write("%s," % str(row[0]))
                f.write("%s," % str(row[1]))
                f.write("%s\n" % str(row[2]))


        # Point query for round robin partition
        dbquery = "select partitionnum from {0} ".format(ROUND_ROBIN_METADATA)
        cursor.execute(dbquery)
        rrpartitioncount = int(cursor.fetchone()[0])


        # for point query we have to find data in all partitions.
        for count in range(rrpartitioncount):
            partitionName = ROUND_ROBIN_OUTPUT + repr(count)
            dbquery = "select * from {0} where rating = {1} ".format(partitionName, ratingValue)
            cursor.execute(dbquery)
            rows = cursor.fetchall()
            with open(POINT_OUTPUT_FILE, 'a+') as f:
                for row in rows:
                    f.write("%s," % partitionName)
                    f.write("%s," % str(row[0]))
                    f.write("%s," % str(row[1]))
                    f.write("%s\n" % str(row[2]))

    except Exception as e:
        print("Exception has occured while processing point query function: ", e)
