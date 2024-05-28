import os
import numpy as np
import time
import tempfile
import subprocess
import sqlite3
#import psycopg2 as pg
#from psycopg2.extras import execute_values
from dlnpyutils import utils as dln
from . import utils

""" Jeeves database code and object """

# Mimic the sqlite3 console shell's .dump command
# Author: Paul Kippes <kippesp@gmail.com>
# https://stackoverflow.com/questions/6677540/how-do-i-dump-a-single-sqlite3-table-in-python
def _iterdump(connection, table_name):
    """
    Returns an iterator to the dump of the database in an SQL text format.

    Used to produce an SQL dump of the database.  Useful to save an in-memory
    database for later restoration.  This function should not be called
    directly but instead called from the Connection method, iterdump().
    """

    cu = connection.cursor()
    table_name = table_name

    yield('BEGIN TRANSACTION;')

    # sqlite_master table contains the SQL CREATE statements for the database.
    q = """
       SELECT name, type, sql
        FROM sqlite_master
            WHERE sql NOT NULL AND
            type == 'table' AND
            name == :table_name
        """
    schema_res = cu.execute(q, {'table_name': table_name})
    for table_name, type, sql in schema_res.fetchall():
        if table_name == 'sqlite_sequence':
            yield('DELETE FROM sqlite_sequence;')
        elif table_name == 'sqlite_stat1':
            yield('ANALYZE sqlite_master;')
        elif table_name.startswith('sqlite_'):
            continue
        else:
            yield('%s;' % sql)

        # Build the insert statement for each row of the current table
        res = cu.execute("PRAGMA table_info('%s')" % table_name)
        column_names = [str(table_info[1]) for table_info in res.fetchall()]
        q = "SELECT 'INSERT INTO \"%(tbl_name)s\" VALUES("
        q += ",".join(["'||quote(" + col + ")||'" for col in column_names])
        q += ")' FROM '%(tbl_name)s'"
        query_res = cu.execute(q % {'tbl_name': table_name})
        for row in query_res:
            yield("%s;" % row[0])

    # Now when the type is 'index', 'trigger', or 'view'
    #q = """
    #    SELECT name, type, sql
    #    FROM sqlite_master
    #        WHERE sql NOT NULL AND
    #        type IN ('index', 'trigger', 'view')
    #    """
    #schema_res = cu.execute(q)
    #for name, type, sql in schema_res.fetchall():
    #    yield('%s;' % sql)

    yield('COMMIT;')

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
        bindata = converttobinarydata(datafile)
        # Convert data into tuple format
        data_tuple = (bindata,)
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
    d2d = {"TEXT":(str,200), "INTEGER":int,
           "REAL":float, "BLOB":object}

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
        # no data type, default is blob
        if len(pair)==1:
            pair.append('blob')
        dt.append( (pair[0], d2d[pair[1].upper()]) )
    dtype = np.dtype(dt)

    # Convert to numpy structured array
    tab = np.zeros(len(data),dtype=dtype)
    tab[...] = data
    del(data)

    if verbose: print('got data in '+str(time.time()-t0)+' sec.')

    return tab

def data_standardize(data,table_columns):
    """
    Standardize data to insert/update database.

    Parameters
    ----------
    data : table/dict/list/tuple
      The data to standardize.
      Formats allowed:
      1) table: Table with column names being the same as the 
           database column names.
      2) dict:  Dictionary with key values for the column names.
           Can also be a list of dicts.
      3) list/tuple: must have the correct number of columns
           and in the right order.  Can also input list of lists
           to insert multiple rows at one time.

    Returns
    -------
    data_tuple
       Tuple-ized data.
    cols : list
       List of column names for the data.

    Examples
    --------

    data_tuple,cols = data_standardize(data,table_columns)

    """

    # The data will be converted to tuples
    # sqlite3 database column names are NOT case sensitive
    
    # 1) Table
    #---------
    if isinstance(data,Table) or (isinstance(data,np.ndarray) and data.dtype.names is not None):
        # Convert astropy table to numpy structured array table
        if isinstance(data,Table):
            data = np.array(data)
        # Convert to tuples
        data_tuple = data.tolist()  # list of tuples
        cols = data.dtype.names
        
    # 2) Dict or list/tuple of dicts
    #-------------------------------
    elif isinstance(data,dict) or ((isinstance(data,list) or isinstance(data,tuple)) and
                                   isinstance(data[0],dict)):
        # Dict
        if isinstance(data,dict):
            data_tuple = [tuple(data.values())]  # single-element list of tuple
            cols = data.keys()
        # list/tuple of dicts
        else:
            # will the dict values always be ordered the same way?
            data_tuple = [tuple(d.values()) for d in data]
            cols = data[0].keys()

    # 3) list/tuples
    #---------------
    elif isinstance(data,list) or isinstance(data,tuple):
        cols = table_columns
        # Single list/tuple
        if isinstance(data[0],list)==False and isinstance(data[0],tuple)==False:
            # Check number of columns
            if len(data) != len(table_columns):
                raise ValueError('Number of list elements does not match number of columns')
            # Convert list to tuple
            if isinstance(data,list):
                data_tuple = [tuple(data)]   # single-element list of tuple
            else:
                data_tuple = [data]          # single-element list of tuple
        # List of lists/tuples
        else:
            data_tuple = [tuple(d) for d in data]
    # Unrecognized format
    else:
        raise ValueError('data format not supported')

    return data_tuple,cols
    
    
class Database(object):
    """
    Jeeves database object
    """

    def __init__(self,filename=':memory:'):
        self.filename = filename

    def __repr__(self):
        """ Print info """
        out = '<Jeeves.Database ['
        if hasattr(self,'filename') and self.filename is not None:
            ntables = len(self.tables())
            if ntables>0:
                sz = self.size()
                if sz>=1e9:
                    osz = '{:.1f}GB'.format(sz/1e9)
                elif sz>=1e6:
                    osz = '{:.1f}MB'.format(sz/1e6)
                elif sz>=1e3:
                    osz = '{:.1f}kB'.format(sz/1e3)                    
                out += str(ntables)+' tables,'+osz+']>\n'  
            out += self.filename+'\n'
        return out

    def __len__(self):
        return self.size()
    
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
        sz = 0
        # Database
        if name is None:
            sql = 'SELECT name, SUM("pgsize") FROM "dbstat" GROUP BY name'
            res = self.query(sql=sql)
        # Table
        else:
            sql = 'SELECT name, SUM("pgsize") FROM "dbstat"'
            sql += " WHERE name='"+name+"'"
            sql += " GROUP BY name"
            res = self.query(sql=sql)
        sz = [r[1] for r in res if r[0]!='sqlite_schema']
        if len(sz)==0:
            return None
        sz = np.sum(sz)
        return sz

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

    def insert(self,table,data,onconflict=None,constraintname=None):
        """
        Insert information into a table.

        Parameters
        ----------
        table : str
           Name of the table.
        data : 
           Data to insert.  There are 3 supported formats:
            1) table: Table with column names being the same as the 
                 database column names.
            2) dict:  Dictionary with key values for the column names.
                 Can also be a list of dicts.
            3) list/tuple: must have the correct number of columns
                 and in the right order.  Can also input list of lists
                 to insert multiple rows at one time.
        onconflict: str, optional
            What to do when there is a uniqueness requirement on the table and there is
              a conflict (i.e. one of the inserted rows will create a duplicate).  The
              options are:
              'update': Update the existing row with the information from the new insert.
              'do nothing': Do nothing, leave the existing row as is and do not insert the
                          new conflicting row.
              'ignore': skips rows with conflicts.
              'replace': replace rows with the new values on conflicts.
              Default is None.
        updateset : str, optional
            Update "SET" statement.
        constraintname : str, optional
            If onconflict='update', then this should be the name of the unique columns
              (comma-separated list of column names).
            Default is None.

        Examples
        --------

        insert('registry',data)
        
        """
        vals = table.split('.')
        if len(vals)==1:
            raise ValueError('')
        # Check that the table exists
        if self.exists(table)==False:
            raise ValueError('table '+str(table)+' not found')
        # Standardize the data input
        table_columns = self.columns(table)
        data_tuple,cols = data_standardize(data,table_columns)
        
        # Given values for some of the columns
        # INSERT INTO table_name (column1, column2, column3, ...)
        # VALUES (value1, value2, value3, ...);

        # Given values for all columns but need to be in the right order
        # INSERT INTO table_name
        # VALUES (value1, value2, value3, ...);

        # Can also insert multiple rows
        # INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country)
        # VALUES
        # ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway'),
        # ('Greasy Burger', 'Per Olsen', 'Gateveien 15', 'Sandnes', '4306', 'Norway'),
        # ('Tasty Tee', 'Finn Egan', 'Streetroad 19B', 'Liverpool', 'L1 0AA', 'UK');

        sql = "INSERT INTO "+table
        sql += '('+','.join(cols)+') VALUES %s'
        # Add "on conflict" statement
        if onconflict is not None:
            sql += ' ON CONFLICT '+onconflict
            if onconflict.lower()=='update' and updateset is not None:
                sql += ' SET '+updateset
        self.cur.execute(sql, data_tuple)

    def update(self,table,data,condition):
        """ Update database."""
        vals = table.split('.')
        if len(vals)==1:
            raise ValueError('')
        # Check that the table exists
        if self.exists(table)==False:
            raise ValueError('table '+str(table)+' not found')

        table_columns = self.columns(table)
        data_tuple,cols = data_standardize(data,table_columns)
        
        # UPDATE table_name
        # SET column1 = value1, column2 = value2, ...
        # WHERE condition;

        # condition can be a list
        if isinstance(condition,str):
            condition = len(data_tuple)*[condition]  # make list
        elif utils.iterable(condition):
            pass
        else:
            raise ValueError('condition type not supported')
        # Loop over entries
        for i in range(len(data_tuple)):
            pairs = [c+'='+d for c,d in zip(cols,data_tuple[i])]
            vals = ','.join(pairs)
            sql = "UPDATE "+table+" SET "+vals+" WHERE "+condition[i]
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
        # 'CREATE TABLE exposure(expnum TEXT, nchips INTEGER, filter TEXT, exptime REAL,
        #                        utdate TEXT, uttime TEXT, airmass REAL, wcstype TEXT)'
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
        # 'CREATE TABLE exposure(expnum TEXT, nchips INTEGER, filter TEXT,
        #                   exptime REAL, utdate TEXT, uttime TEXT, airmass REAL, wcstype TEXT)'
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
        # Should allow to check for an index as well
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

    def sql(self,cmd):
        """ Execute raw sql command or list of sql commands."""
        if isinstance(cmd,str):
            self.cur.execute(cmd)
        elif utils.iterable(cmd):
            for c in cmd:
                self.cur.execute(c)
        else:
            raise ValueError('sql type '+str(type(cmd))+' not supported')
        # Check if we need to commit
        docommit = False
        for c in ['insert','update','delete','merge','call',
                  'explain plan','lock table']:
            if cmd.lower().find(c)>-1:
                docommit = True
        if docommit:
            self.db.commit()
        # Do we need to fetch or return anything?
        if cmd.lower().find('select ')>-1:
            data = self.cur.fetchall()
            return data
        
    def create(self,name,fmt,extra=None):
        """
        Create table or column.

        Parameters
        ----------
        name : str
           Name of the table or name of the column (in table.column format).
        fmt : str
           For a column this is the format type (e.g. real, text).  For a
             table this is the list of columns and data types pairs, e.g.,
             fmt=[('ra','real'),('dec','real'),('id','text')].
             You can add an extra element for each column that gives extra
             text like a default value or not null (e.g., 'DEFAULT "store1" NOT NULL').
        extra : str
           Extra conditions to add to the table creation string.

        Examples
        --------

        create('registry','')

        create('registry.ra','real')

        """
        vals = name.split('.')
        if len(vals)>2:
            raise ValueError('Only TABLE or TABLE.COLUMN format supported')
        # Column
        if len(vals)==2:
            table = vals[0]
            column = vals[1]
            # fmt is the column type e.g., TEXT, REAL, etc.
            # and possible extra requirements or defaults
            if isinstance(fmt,list) or isinstance(fmt,tuple):
                sfmt = ' '.join(fmt)
            else:
                sfmt = fmt
            sql = "ALTER TABLE "+table+" ADD COLUMN "+column+" "+sfmt
            self.cur.execute(sql)
            self.db.commit()
        # Table
        else:
            table = vals[0]
            # Generate fmt string from list of columns and data types
            # and any extra requirements
            sfmt = ','.join([' '.join(f) for f in fmt])            
            sql = "CREATE TABLE "+table+"("+sfmt+")"
            # add extra conditions for the table
            if extra is not None:
                sql += ' '+extra
            self.cur.execute(sql)
            self.db.commit()            

    def delete(self,table,condition):
        """
        Delete rows in a table.
        """
        sql = 'DELETE FROM '+table+' WHERE '+condition
        self.cur.execute(sql)
        self.db.commit()
        
    def drop(self,name,fmt):
        """
        Drop/delete a table or column.

        Parameters
        ----------
        name : str
           Name of the table or name of the column (in table.column format).

        Examples
        --------

        drop('registry')

        drop('registry.ra')

        """
        vals = name.split('.')
        if len(vals)>2:
            raise ValueError('Only TABLE or TABLE.COLUMN format supported')
        # Column
        if len(vals)==2:
            table = vals[0]
            column = vals[1]
            sql = "ALTER TABLE "+table+" DROP COLUMN "+column
            self.cur.execute(sql)
            self.db.commit()
        # Table
        else:
            table = vals[0]
            sql = "DROP TABLE "+table
            self.cur.execute(sql)
            self.db.commit()  
            
    def analyze(self,table,verbose=False):
        """ Analyze table."""
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

    def dump(self,filename,table=None,unix=True):
        """ Dump database to a file."""
        # Use sqlite3 unix commands
        if unix:
            lines = []
            lines += ['.output '+filename]
            if table is not None:
                lines += ['.dump '+table]
            else:
                lines += ['.dump']
            lines += ['.exit']

            # Write to temporate file
            fd,tfile = tempfile.mkstemp(suffix='.sql',prefix='dump',text=True)
            dln.writelines(tfile,lines)
        
            # Execute sqlite3 command
            cmd = "sqlite3 "+self.filename+" '.read "+tfile+"'"
            res = subprocess.run(cmd,capture_output=True,shell=True)

            # Delete temporary file
            if os.path.exists(tfile):
                os.remove(tfile)
            
        # Use python
        else:
            # https://stackoverflow.com/questions/6677540/how-do-i-dump-a-single-sqlite3-table-in-python
            if table is not None:
                tables = [table]
            else:
                tables = self.tables()
            with open(filename, 'w') as f:
                for t in tables:
                    for line in _iterdump(self.db, t):
                        f.write('%s\n' % line)        
