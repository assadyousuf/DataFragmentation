#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    
    cur = openconnection.cursor()


    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    cur.execute("CREATE TABLE "+ratingstablename+" (row_id serial primary key,UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating REAL, temp5 VARCHAR(10), Timestamp INT)")
    loadout = open(ratingsfilepath,'r')
    cur.copy_from(loadout,ratingstablename,sep = ':',columns=('UserID','temp1','MovieID','temp3','Rating','temp5','Timestamp'))
    cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")
   
        
    openconnection.commit()
    cur.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
	
    range=5.0/numberofpartitions
    j=0
    part=0
    global RangePart 
    RangePart = numberofpartitions

    #Create a meta table
    cur.execute("DROP TABLE IF EXISTS rangePartition_info")
    cur.execute("CREATE TABLE rangePartition(numberofpartitions INT)")
    cur.execute("TRUNCATE TABLE rangePartition")	
    cur.execute("INSERT INTO rangePartition VALUES (" + str(numberofpartitions) + ") "  )

	
    while j<5.0:
        if j == 0:
            cur.execute("DROP TABLE IF EXISTS range_part"+str(part))
            cur.execute("CREATE TABLE range_part"+str(part)+ " AS SELECT * FROM "+ratingstablename+" WHERE Rating>="+str(j)+ " and Rating<="+str(j+range))
            j = j + range
            part=part + 1
        else:
            cur.execute("DROP TABLE IF EXISTS range_part"+str(part))
            cur.execute("CREATE TABLE range_part"+str(part)+" AS SELECT * FROM "+ratingstablename+" WHERE Rating>"+str(j)+ " and Rating<="+str(j+range))
            j=j + range
            part=part + 1
	    
	
	openconnection.commit()
    cur.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):

    cur = openconnection.cursor()

    list_partitions = list(range(numberofpartitions))


    cur.execute("DROP TABLE IF EXISTS rrobin_info")
    cur.execute("CREATE TABLE rrobin_info (partition_number INT, numberofpartitions INT)")


    end=-1

    for part in list_partitions:
        cur.execute("DROP TABLE IF EXISTS rrobin_part" + str(part))
        cur.execute("CREATE TABLE rrobin_part" + str(part) + " AS SELECT userid,movieid,rating FROM  (SELECT userid,movieid,rating,row_number() over() AS row_num FROM " + str(ratingstablename) + ") a where (a.row_num -1 + " + str(numberofpartitions) + ")% " + str(numberofpartitions) + " = " + str(part) )
        end=part
        


    cur.execute("truncate TABLE rrobin_info")	
    cur.execute("INSERT INTO rrobin_info VALUES (" + str(end) + "," + str(numberofpartitions) +") "  )
    
    openconnection.commit()
    cur.close()



def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
   	
    cur = openconnection.cursor()
    cur.execute("SELECT partition_number,numberofpartitions FROM rrobin_info")
    f = cur.fetchone()
    part = f[0]
    numberofpartitions = f[1]



    cur.execute("INSERT INTO rrobin_part" + str((part + 1)%numberofpartitions) + " VALUES( " + str(userid) + "," + str(itemid) + "," + str(rating) + ") ")
    cur.execute("INSERT INTO ratings VALUES( " + str(userid) + "," + str(itemid) + "," + str(rating) + ") ")
    part = (part + 1)%numberofpartitions
    cur.execute("truncate table rrobin_info")	
    cur.execute("INSERT INTO rrobin_info VALUES (" + str(part) + "," + str(numberofpartitions) +") "  )

    openconnection.commit()
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    
    cur = openconnection.cursor()
    global RangePart

    range = 5.0 / RangePart
    Start = 0
    partitionnumber = 0
    End = range

    while Start<5.0:
        if Start == 0:
            if rating >= Start and rating <= End:
                break
            partitionnumber = partitionnumber + 1
            Start = Start + range
            End = End + range
        else: 
            if rating > Start and rating <= End:
                break
            partitionnumber = partitionnumber + 1
            Start = Start + range
            End = End + range
            
            
            
    cur.execute("INSERT INTO range_part"+str(partitionnumber)+" (UserID,MovieID,Rating) VALUES (%s, %s, %s)",(userid, itemid, rating))
    openconnection.commit()
    cur.close()

def createDB(dbname='dds_assignment'):
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
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT ratingstablename FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT ratingstablename FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for ratingstablename in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError:
        if openconnection:
            openconnection.rollback()
        print ('Error %s') 
    except IOError:
        if openconnection:
            openconnection.rollback()
        print ('Error %s')
    finally:
        if cursor:
            cursor.close()
