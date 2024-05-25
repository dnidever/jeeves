import os
import numpy as np
import time
import sqlite3
from dlnpyutils import utils as dln
from . import utils

""" Jeeves database code and object """

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

def writetab(tab,dbfile,table='meas'):
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
    if isinstance(dbfile,str):
        db = opendb(dbfile)
    elif isinstance(dbfile,sqlite3.Connection):
        db = dbfile
    else:
        raise RaiseValue('dbfile input not supported')
    c = db.cursor()
    # Index the table
    if verbose: print('Analyzing '+table)
    c.execute('ANALYZE '+table)
    data = c.fetchall()
    #db.close()
    if verbose: print('analyzing done after '+str(time.time()-t0)+' sec')
    
def query(dbfile,table='registry',cols='*',where=None,raw=False,
          groupby=None,limit=None,verbose=False):
    """ Get rows from the database """
    t0 = time.time()
    if isinstance(dbfile,str):
        db = opendb(dbfile)
    elif isinstance(dbfile,sqlite3.Connection):
        db = dbfile
    else:
        raise RaiseValue('dbfile input not supported')
    cur = db.cursor()

    # Convert numpy data types to sqlite3 data types
    d2d = {"TEXT":(np.str,200), "INTEGER":np.int,
           "REAL":np.float, "BLOB":object}

    # Start the SELECT statement
    cmd = 'SELECT '+cols+' FROM '+table
    # Add WHERE statement
    if where is not None:
        cmd += ' WHERE '+where
    # Add GROUP BY statement
    if groupby is not None:
        cmd += ' GROUP BY '+groupby
    # Add LIMIT
    if limit is not None:
        cmd += ' LIMIT '+str(limit)
        
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
    res = cur.fetchall()
    #db.close()
    head = res[0][0]
    # 'CREATE TABLE exposure(expnum TEXT, nchips INTEGER, filter TEXT, exptime REAL, utdate TEXT, uttime TEXT, airmass REAL, wcstype TEXT)'
    lo = head.find('(')
    hi = head.find(')')
    head = head[lo+1:hi]
    columns = head.split(',')
    columns = dln.strip(columns)
    dt = []
    for c in columns:
        pair = c.split(' ')
        dt.append( (pair[0], d2d[pair[1].upper()]) )
    dtype = np.dtype(dt)

    # Convert to numpy structured array
    tab = np.zeros(len(data),dtype=dtype)
    tab[...] = data
    del(data)

    if verbose: print('got data in '+str(time.time()-t0)+' sec.')

    return tab


class Database(object):
    """
    Jeeves database object
    """

    def __init__(self,filename):
        self.filename = filename

    def __repr__(self):
        """ Print info """
        out = '<Jeeves.Database>\n'
        if hasattr(self,'filename') and self.filename is not None:
            out += self.filename+'\n'
        return out
        
    def open(self):
        """ Open the database for reading/writing."""
        if self.filename is None:
            raise ValueError('No database filename')
        if self._db is None:
            self._db = opendb(self.filename)
        
    def close(self):
        """ Close the database."""
        if self._db is not None:
            if self.cur is not None:
                self.cur.close()  # close cursor first
            self._db.close()

    def size(self,name=None):
        """ Return size of database or table."""
        # Database
        if name is None:
            pass
        # Table
        else:
            pass
 
    @property
    def db(self):
        """ Return the db object."""
        if hasattr(self,'_db')==False:
            self._db = None
        if self._db is None:
            self.open()
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

    def schema(self,table=None):
        """ Return the schema."""
        res = []
        # Full database
        if table is None:
            res = []
            for t in self.tables():
                res += self.schema(t)
        # Single table
        else:
            res += [table]
            cols = self.columns(table)
            res += [table+'.'+c for c in cols]
        return res
        
    def query(self,sql=None,table=None,cols='*',where=None,
              raw=False,groupby=None,limit=None,verbose=False):
        """ Run a query."""
        # SQL text input
        if sql is not None:
            self.cur.execute(sql)
            res = self.cur.fetchall()
            return res
        # Separated information input
        else:
            if table is None:
                raise ValueError('Need table for query')
            if self.exists(table)==False:
                raise ValueError(str(table)+' table does not exist')
            return query(self.db,table=table,cols=cols,where=where,raw=raw,
                         groupby=groupby,limit=limit,verbose=verbose)

    def insert(self,name,key,data,update=True):
        """ Insert information into a table. """
        vals = name.split('.')
        if len(vals)==1:
            raise ValueError('')
        sql = "INSERT INTO "+table+" VALUES ("+data+")"
        self.cur.execute(sql)
        self.db.commit()

    def dtype(self,table):
        """ Return table column dtype. """
        if self.exists(table)==False:
            return None
        # Get table column names and data types
        self.cur.execute("select sql from sqlite_master where tbl_name = '"+table+"' and type='table'")
        res = self.cur.fetchall()
        head = res[0][0]
        # 'CREATE TABLE exposure(expnum TEXT, nchips INTEGER, filter TEXT, exptime REAL, utdate TEXT, uttime TEXT, airmass REAL, wcstype TEXT)'
        lo = head.find('(')
        hi = head.find(')')
        head = head[lo+1:hi]
        columns = head.split(',')
        columns = dln.strip(columns)
        # Convert sqlite3 data types to numpy data types
        d2d = {"TEXT":"S", "INTEGER":"i", "REAL":"f", "BLOB":"O"}
        dt = []
        for c in columns:
            pair = c.split(' ')
            dt.append( (pair[0], d2d[pair[1].upper()]) )
        dtype = np.dtype(dt)
        return dtype

    def tables(self):
        """ Return table names."""
         # Get table names
        self.cur.execute("select name from sqlite_master where type='table'")
        res = self.cur.fetchall()
        if len(res)==0:
            return None
        else:
            res = [r[0] for r in res]
        return res
    
    def columns(self,table):
        """ Return table columns."""
        if self.exists(table)==False:
            return None
        # Get table column names
        self.cur.execute("select sql from sqlite_master where tbl_name = '"+table+"' and type='table'")
        res = self.cur.fetchall()
        head = res[0][0]
        # 'CREATE TABLE exposure(expnum TEXT, nchips INTEGER, filter TEXT, exptime REAL, utdate TEXT, uttime TEXT, airmass REAL, wcstype TEXT)'
        lo = head.find('(')
        hi = head.find(')')
        head = head[lo+1:hi]
        columns = head.split(',')
        columns = dln.strip(columns)
        cols = []
        for c in columns:
            pair = c.split(' ')
            cols.append(pair[0])
        return cols
        
    def exists(self,name):
        """ Check if the table or name exists in the database."""
        # e.g., table or table.column
        vals = name.split('.')
        if len(vals)>2:
            raise ValueError('Only TABLE or TABLE.COLUMN format supported')
        # Table only
        if len(vals)==1:
            table = vals[0]
            sql = 'SELECT name from sqlite_master where type= "table" and name="'+table+'"'
            res = self.query(sql)
            if len(res)>0:
                return True
            else:
                return False
        # Table and column input
        else:
            table = vals[0]
            column = vals[1]
            # First check that table exists
            sql = 'SELECT * from sqlite_master where type= "table" and name="'+table+'"'
            res = self.query(sql)
            if len(res)==0:
                return False
            # Then check if the column exists
            cols = self.columns(table)
            if column in cols:
                return True
            else:
                return False

    def create(self,name,fmt):
        """ Create table or column."""
        vals = name.split('.')
        if len(vals)>2:
            raise ValueError('Only TABLE or TABLE.COLUMN format supported')
        # Column
        if len(vals)==2:
            table = vals[0]
            column = vals[1]
            # fmt is the column type e.g., TEXT, REAL, etc.
            sql = "ALTER TABLE "+table+" ADD COLUMN "+column+" "+fmt
            self.cur.execute(sql)
            self.db.commit()
        # Table
        else:
            table = vals[0]
            sql = "CREATE TABLE "+table+"("+fmt+")"            
            self.cur.execute(sql)
            self.db.commit()            

    def analyze(self,table,verbose=True):
        """ Analyze table."""
        vals = name.split('.')
        if len(vals)!=1:
            raise ValueError('Only TABLE format supported')
        table = vals[0]
        t0 = time.time()
        # Index the table
        if verbose: print('Analyzing '+table)
        self.cur.execute('ANALYZE '+table)
        data = self.cur.fetchall()
        if verbose: print('analyzing done after {:.1f} sec'.format(time.time()-t0))

    def createindex(self,name):
        """ Create index on column."""
        vals = name.split('.')
        if len(vals)!=2:
            raise ValueError('Only TABLE.COLUMN format supported')
        table,column = vals
        t0 = time.time()
        index_name = 'idx_'+column+'_'+table
        # Check if the index exists first
        self.cur.execute('select name from sqlite_master')
        d = self.cur.fetchall()
        for nn in d:
            if nn[0]==index_name:
                print(index_name+' already exists')
                return
        # Create the index
        if verbose: print('Indexing '+name)
        if unique:
            c.execute('CREATE UNIQUE INDEX '+index_name+' ON '+table+'('+column+')')
        else:
            c.execute('CREATE INDEX '+index_name+' ON '+table+'('+column+')')
        data = self.cur.fetchall()
        if verbose: print('indexing done after {:.1f} sec'.format(time.time()-t0))

    def dump(self,filename):
        """ Dump database to a file."""
        pass
