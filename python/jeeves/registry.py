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
        # Need to make columns for the meta-data
        
        # Make foreign key between registry and data tables
        
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
