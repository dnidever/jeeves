#!/usr/bin/env python

import os
import numpy as np
from dlnpyutils import utils as dln
import time
import sqlite3
from . import utils

# Jeeves registry database

def keyize(key):
    """ Make key into a single string."""
    if utils.iterable(key):
        keylist = [str(k) for k in key]
        skey = '----'.join(keylist)
    else:
        skey = str(key)
    return skey

def converttobinarydata(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        blobData = file.read()
    return blobData

def writebinarytofile(data,filename):
    # Write binary format to file
    with open(filename, 'wb') as file:
        file.write(data)

def insertblob(dbfile,key,datafile,column='blob',table='blobdata'):
    """ Insert binary data from file into database """
    try:
        db = sqlite3.connect(dbfile)
        cursor = db.cursor()
        sql = "INSERT INTO "+table
        sql += "("+column+") VALUES (?)"
        empdata = converttobinarydata(datafile)
        # Convert data into tuple format
        data_tuple = (empdata,)
        cursor.execute(sql, data_tuple)
        db.commit()
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to insert blob data into sqlite table", error)
    finally:
        if db:
            db.close()


def getblob(dbfile,key,column='blob',table='blobname'):
    """ Retrieve blob data from database """
    db = sqlite3.connect(dbfile)
    cursor = db.cursor()
    sql = "SELECT "+column+" FROM "+table+" WHERE name='"+key+"'"
    cursor.execute(sql)
    res = cursor.fetchall()
    cursor.close()
    data = res[0][0]
    db.close()
    return data
            
def opendb(dbfile):
    """ Open database and add adapters """
    sqlite3.register_adapter(np.int8, int)
    sqlite3.register_adapter(np.int16, int)
    sqlite3.register_adapter(np.int32, int)
    sqlite3.register_adapter(np.int64, int)
    sqlite3.register_adapter(np.float16, float)
    sqlite3.register_adapter(np.float32, float)
    sqlite3.register_adapter(np.float64, float)
    db = sqlite3.connect(dbfile, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    return db

def writecat(tab,dbfile,table='meas'):
    """ Write a catalog to the database """
    ncat = dln.size(tab)
    db = opendb(dbfile)
    c = db.cursor()

    # Convert numpy data types to sqlite3 data types
    d2d = {"S":"TEXT", "i":"INTEGER", "f":"REAL"}

    # Get the column names
    cnames = tab.dtype.names
    cdict = dict(tab.dtype.fields)
    # Create the table
    #   the primary key ROWID is automatically generated
    if len(c.execute('SELECT name from sqlite_master where type= "table" and name="'+table+'"').fetchall()) < 1:
        columns = cnames[0].lower()+' '+d2d[cdict[cnames[0]][0].kind]
        for n in cnames[1:]: columns+=', '+n.lower()+' '+d2d[cdict[n][0].kind]
        c.execute('CREATE TABLE '+table+'('+columns+')')
    # Insert statement
    columns = []
    for n in cnames: columns.append(n.lower())
    qmarks = np.repeat('?',dln.size(cnames))
    c.executemany('INSERT INTO '+table+'('+','.join(columns)+') VALUES('+','.join(qmarks)+')', list(tab))
    db.commit()
    db.close()

def createindex(dbfile,col='measid',table='meas',unique=True,verbose=False):
    """ Index a column in the database """
    t0 = time.time()
    db = sqlite3.connect(dbfile, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    c = db.cursor()
    index_name = 'idx_'+col+'_'+table
    # Check if the index exists first
    c.execute('select name from sqlite_master')
    d = c.fetchall()
    for nn in d:
        if nn[0]==index_name:
            print(index_name+' already exists')
            return
    # Create the index
    if verbose: print('Indexing '+col)
    if unique:
        c.execute('CREATE UNIQUE INDEX '+index_name+' ON '+table+'('+col+')')
    else:
        c.execute('CREATE INDEX '+index_name+' ON '+table+'('+col+')')
    data = c.fetchall()
    db.close()
    if verbose: print('indexing done after '+str(time.time()-t0)+' sec')

def analyzetable(dbfile,table,verbose=False):
    """ Run analyze command on a table.  This speeds up queries."""
    t0 = time.time()
    db = sqlite3.connect(dbfile, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    c = db.cursor()
    # Index the table
    if verbose: print('Analyzing '+table)
    c.execute('ANALYZE '+table)
    data = c.fetchall()
    db.close()
    if verbose: print('analyzing done after '+str(time.time()-t0)+' sec')
    
def query(dbfile,table='meas',cols='*',where=None,groupby=None,raw=False,verbose=False):
    """ Get rows from the database """
    t0 = time.time()
    db = opendb(dbfile)
    cur = db.cursor()

    # Convert numpy data types to sqlite3 data types
    d2d = {"TEXT":(np.str,200), "INTEGER":np.int, "REAL":np.float}

    # Start the SELECT statement
    cmd = 'SELECT '+cols+' FROM '+table

    # Add WHERE statement
    if where is not None:
        cmd += ' WHERE '+where

    # Add GROUP BY statement
    if groupby is not None:
        cmd += ' GROUP BY '+groupby
        
    # Execute the select command
    if verbose:
        print('CMD = '+cmd)
    cur.execute(cmd)
    data = cur.fetchall()

    # No results
    if len(data)==0:
        return np.array([])

    # Return the raw results
    if raw is True:
        return data
    
    # Get table column names and data types
    cur.execute("select sql from sqlite_master where tbl_name = '"+table+"' and type='table'")
    dum = cur.fetchall()
    db.close()
    head = dum[0][0]
    # 'CREATE TABLE exposure(expnum TEXT, nchips INTEGER, filter TEXT, exptime REAL, utdate TEXT, uttime TEXT, airmass REAL, wcstype TEXT)'
    lo = head.find('(')
    hi = head.find(')')
    head = head[lo+1:hi]
    columns = head.split(',')
    columns = dln.strip(columns)
    dt = []
    for c in columns:
        pair = c.split(' ')
        dt.append( (pair[0], d2d[pair[1]]) )
    dtype = np.dtype(dt)

    # Convert to numpy structured array
    tab = np.zeros(len(data),dtype=dtype)
    tab[...] = data
    del(data)

    if verbose: print('got data in '+str(time.time()-t0)+' sec.')

    return tab




class Registry(object):
    """
    Jeeves registry or database
    """

    def __init__(self,configfile):
        self.configfile = None
        self.database_filename = None
        self.datamodel_filename = None
        return
        self.configfile = configfile
        # Load config file
        if os.path.exists(configfile)==False:
            raise FileNotFoundError(configfile)
        self.config = utils.read_config(configfile)
        self.database_filename = self.config['database_filename']
        self.datamodel_filename = self.config['datamodel_filename']
        # initialize
        #self._db = None
        #self._cur = None

    def __repr__(self):
        """ Print info """
        out = '<Jeeves.Registry>'
        if hasattr(self,'configfile') and self.configfile is not None:
            out += self.configfile
        return out
        
    def opendb(self):
        """ Open the database for reading/writing."""
        if self.database_filename is None:
            raise ValueError('No database filename')
        if self._db is None:
            self._db = opendb(self.database_filename)
        
    def closedb(self):
        """ Close the database."""
        if self._db is not None:
            if self.cur is not None:
                self.cur.close()  # close cursor first
            self._db.close()

    
            
    def initregistrytable(self):
        """ Initialize registry table."""
        self.cur.execute('CREATE TABLE registry (key text, filename text)')
        self.db.commit()
 
    @property
    def db(self):
        """ Return the db object."""
        if hasattr(self,'_db')==False:
            self._db = None
        if self._db is None:
            self.opendb()
        return self._db
            
    @property
    def cur(self):
        """ Return the cursor object."""
        # No cursor, create it
        if hasattr(self,'_cur')==False:
            self._cur = None
        if self._cur is None:
            self._cur = self.db.cursor()
        return self._cur

    def search(self,key):
        """ Search for the key in the table."""
        skey = keyize(key)
        sql = "select * from registry where key='"+skey+"'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        return res

    def exists(self,key):
        """ Check if the key exists in the registry."""
        res = self.search(key)
        if len(res)>0:
            return True
        else:
            return False

    def retrieve(self,key):
        """ Retrieve the filename for a key."""
        res = self.search(key)
        if len(res)==0:
            return None
        else:
            return res['filename']

    def add(self,key,filename):
        """ Add data to the registry."""
        skey = keyize(key)
        sql = "insert into registry (key,filename) values ('"+skey+"','"+filename+"')"
        self.cur.execute(sql)
        self.cur.commit()

    def delete(self,key,hard=True):
        """ Delete data from registry."""
        skey = keyize(key)
        sql = "select * from registry where key='"+skey+"'"
        self.cur.execute(sql)
        res = cur.fetchall()
        if len(res)==0:
            return
        filename = res['filename']
        # Delete the row in the reistry
        sql = "delete from registry where key='"+skey+"'"
        self.cur.execute(sql)
        self.cur.commit()
        # Delete file as well
        if hard:
            if os.path.exists(filename):
                os.remove(filename)
