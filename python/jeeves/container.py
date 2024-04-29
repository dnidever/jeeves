import os
import numpy as np
import copy
from astropy.io import fits
from astropy.wcs import WCS
from astropy.table import Table

# Data containers

def getdatatype(data):
    if data is not None:
        if isinstance(data,Table) or isinstance(data,fits.fitsrec.FITS_rec):
            datatype = 'table'
        elif isinstance(data,np.ndarray):
            if data.dtype.names is None:
                datatype = 'ndarray'
            else:
                datatype = 'table'
        else:
            datatype = data.__class__.__name__
    else:
        datatype = 'None'
    return datatype

def dowehavewcs(inp):
    """ Determine if a header has a real WCS. """
    if isinstance(inp,WCS):
        w = inp
    else:
        w = WCS(inp)
    if w.has_celestial or w.has_spectral or w.has_temporal:
        return True
    return False
        
    #if head.get('CTYPE1') is None:
    #    return False
    #if head.get('CRVAL1') is None:
    #    return False
    #if head.get('CRPIX1') is None:
    #    return False
    #if head.get('CDELT1') is None and head.get('CD1_1') is None:
    #    return False
    #if head.get('NAXIS') is None or head.get('NAXIS')==0:
    #    return False
    #if head.get('NAXIS1') is None or head.get('NAXIS1')==0:
    #    return False

    
class HistoryList(object):
    """" Contain history """

    def __init__(self,header):
        # Parse through the history and grab all of
        # the HISTORY and COMMENT lines
        history = []
        for k,v,c in header.cards:
            if k == 'HISTORY':
                history.append(v)
        self.data = history

    # Add methods
    def __repr__(self):
        """ Represent the object """
        prefix = self.__class__.__name__ + '('
        if self.data is not None:
            body = np.array2string(self.data, separator=', ', prefix=prefix)
        else:
            bodu = 'None'
        out = ''.join([prefix, body, ')']) +'\n'
        return out  

    def to_header(self):
        """ Make a FITS header """
        head = fits.Header() # blank header to start
        for d in self.data:
            head.add_history(d)
        return head
    
    def copy(self):
        """ Make a copy """
        return copy.deepcopy(self)
    
class MetaData(object):
    """ Container for metadata/header """

    def __init__(self,header=None):
        self.header = header
        if header is not None:
            self._make_properties()
        # Create WCS object if it exists
        self.wcs = WCS(header)
        self.history = HistoryList(header)
            
    def _make_properties(self):
        """ Make properties for each header key """
        for k,v in self.header.items():
            if k not in ['HISTORY','COMMENT','SIMPLE','EXTEND','XTENSION']:
                setattr(self,k.lower(),v)

    def items(self):
        """ Get key/value pairs """
        keystoignore = ['wcs','to_header','header','history',
                        'items','copy','haswcs','info']
        out = []
        for d in dir(self):
            if d[0] != '_' and d not in keystoignore:
                out.append((d,getattr(self,d)))
        return out

    @property
    def haswcs(self):
        """ Do we have a valid WCS. """
        return dowehavewcs(self.wcs)
    
    def __repr__(self):
        """ Represent the object """
        out = self.__class__.__name__ + '('
        items = self.items()
        out += str(len(items)) + ' items)\n'
        return out  

    def info(self):
        """ Print out useful information """
        out = self.__class__.__name__ + '('
        items = self.items()
        out += str(len(items)) + ' items)\n'
        #for n in ['date-obs','ra','dec','object']:
        #    if hasattr(self,n):
        #        out += n+': '+str(getattr(self,n))
        for d in items:
            out += d[0]+' = ' + repr(d[1]) + '\n'
        # If there is a WCS, then print it out
        if self.haswcs:
            out += 'wcs: ' + repr(self.wcs)
        print(out)
    
    def to_header(self):
        """ Make a FITS header """
        if self.header is not None:
            head = self.header.copy()  # start with initial header
        else:
            head = fits.Header()
        # Delete all history lines
        try:
            del head['HISTORY']
        except:
            pass
        # Update all key/value pairs
        for d in self.items():
            head[d[0]] = d[1]
        # wcs to header
        if self.haswcs:
            whead = self.wcs.to_header()
            head += whead
        # Add history
        histhead = self.history.to_header()
        head += histhead
        return head

    def copy(self):
        """ Make a copy """
        return copy.deepcopy(self)

    @classmethod
    def read(cls,filename,exten=0):
        """
        Read a file
        """
        if os.path.exists(filename)==False:
            raise FileNotFoundError(filename)
        head = fits.getheader(filename,exten)
        return MetaData(head)
        
    def write(self,filename,overwrite=False):
        """
        Write to a file
        """
        # construct a header
        head = self.to_header()
        # write to file
        hdulist.writeto(filename,data=None,header=head,overwrite=overwrite)
    
class DataBucket(object):
    """ Container for data and metadata/header """
    
    def __init__(self,data=None,header=None,name=None):
        self.data = data
        self.meta = MetaData(header)
        self.name = name
        self.datatype = getdatatype(data)
                
    def __repr__(self):
        """ Represent the object """
        out = self.__class__.__name__ + '('
        if self.name is not None:
            out += 'name=' + self.name + ', '
        out += 'type=' + self.datatype + ', '
        if self.data is not None:
            if self.datatype == 'table':
                out += 'size=' + str(len(self.data)) + 'R x '
                out += str(len(self.data.dtype.names)) + 'C)\n'
            else:
                out += 'size=' + repr(self.data.T.shape) + ')\n'
        else:
            out += 'size=None)\n'
        return out  

    def info(self):
        """ Show more detailed information """
        out = self.__class__.__name__ + '('
        if self.name is not None:
            out += 'name=' + self.name + ', '
        out += 'type=' + self.datatype + ', '
        if self.data is not None:
            if self.datatype == 'table':
                out += 'size=' + str(len(self.data)) + 'R x '
                out += str(len(self.data.dtype.names)) + 'C)\n'
            else:
                out += 'size=' + repr(self.data.T.shape) + ')\n'
        else:
            out += 'size=None)\n'
        if self.data is not None:
            body = np.array2string(self.data, separator=', ')
        else:
            body = 'None'
        out += 'data = ' + body + '\n'
        print(out)
        self.meta.info()

    @property
    def haswcs(self):
        """ Do we have a WCS """
        return self.meta.haswcs
    
    def to_hdulist(self):
        """ Make a HDUList """
        if isinstance(self.data,Table):
            hdu = fits.table_to_hdu(self.data)
            hdu.header += self.meta.to_header()
        else:
            hdu = fits.ImageHDU(data=self.data,
                                header=self.meta.to_header())
        if self.name is not None:
            hdu.header['extname'] = self.name
        return hdu

    def copy(self):
        """ Make a copy """
        return copy.deepcopy(self)

    @classmethod
    def read(cls,filename,exten=0):
        """
        Read from a fits file
        """
        if os.path.exists(filename)==False:
            raise FileNotFoundError(filename)
        hdu = fits.open()
        if exten>=len(hdu):
            raise IndexError('Extension number too large')
        data = hdu[exten].data.copy()
        head = hdu[exten].header.copy()
        hdu.close()
        return Databucket(data,head)
        
    def write(self,filename,overwrite=False):
        """
        Write to a file
        """
        # construct HDUList
        hdu = self.to_hdulist()
        # write to file
        hdu.writeto(filename,overwrite=overwrite)
    
class DataContainer(object):

    def __init__(self,init=None):

        # how about using the asdf format directly?
        self._bucketnames = []

        # Empty container
        if init is None:
            return
        
        # Input types
        # Filename
        if isinstance(init,str):
            hdulist = fits.open(init)
            bucketnames = self._from_hdulist(hdulist)
            hdulist.close()
            self.filename = init
            self._bucketnames = bucketnames
        # HDUList
        elif isinstance(init,fits.HDUList):
            bucketnames = self._from_hdulist(init)
            self._bucketnames = bucketnames
        # Some data
        else:
            db = DataBucket(init)
            self.add_bucket(db)

    def _from_hdulist(self,hdulist):
        """ Create DataContainer from HDUList """
        bucketnames = []
        for i in range(len(hdulist)):
            # Primary HDU is special            
            if i==0:
                name = 'primary'
            # Extensions
            else:
                name = hdulist[i].header.get('extname')
                if name is None:
                    name = 'exten'+str(i)
            db = DataBucket(hdulist[i].data,hdulist[i].header,name)
            setattr(self,name,db)
            bucketnames.append(name)
        return bucketnames
            
    def __len__(self):
        return self.nbuckets

    def __setitem__(self,index,bucket):
        # Index can be integer or str (name)
        if isinstance(bucket,DataBucket)==False:
            raise ValueError('Must be DataBucket')
        if isinstance(index,int):
            if index>=self.nbuckets:
                raise IndexError('Index too large')
            name = self.bucketnames[index]
            setattr(self,name,bucket)
        elif isinstance(index,str):
            if index in self.bucketnames:
                setattr(self,index,bucket)
            else:
                raise IndexError(str(index)+' not found')
        else:
            raise IndexError('Index must be integer or string name')
    
    def __getitem__(self,index):
        """
        Return a separate spectral order
        Index can be:
        1) integer
        2) string of bucket names
        3) slice
        """
        if isinstance(index,int):
            if index>=self.nbuckets:
                raise IndexError('Index is too large')
            name = self.bucketnames[index]
            return getattr(self,name)
        elif isinstance(index,str):
            if index in self.bucketnames:
                return getattr(self,index)
            else:
                raise ValueError(str(index)+' not found')
        elif isinstance(index,slice):
            # Return a new DataContainer with these DataBuckets
            start1,stop1,step1 = index.indices(self.nbuckets)
            names = self.bucketnames[index]
            new = DataContainer()
            for n in names:
                new.add_bucket(getattr(self,n).copy())
            return new
            
    def __iter__(self):
        self._count = 0
        return self

    def __next__(self):
        if self._count < self.nbuckets:
            self._count += 1    
            return self[self._count-1]
        else:
            raise StopIteration

    def __add__(self, value):
        new = self.copy()
        new.add_bucket(value)
        return new

    def __iadd__(self, value):
        self.add_bucket(value)
        return self

    def __radd__(self, value):
        return self + value

    def add_bucket(self,bucket,name=None):
        """ Add a new data bucket."""
        if isinstance(bucket,DataBucket)==False:
            raise ValueError('Can only add DataBuckets')
        if name is None:
            if hasattr(bucket,'name') and bucket.name is not None:
                name = bucket.name
            else:
                if self.nbuckets==0:
                    name = 'primary'
                else:
                    names = self.bucketnames
                    extnum = [int(e[5:]) for e in names if e.startswith('exten')]
                    name = 'exten'+str(np.max(extnum)+1)
                    print('Adding',name)
        if self.index(name) != -1:
            raise ValueError(str(hame)+' already taken')
        setattr(self,name,bucket)
        self._bucketnames.append(name)

    def del_bucket(self,index):
        """ Delete a bucket """
        if isinstance(index,int)==False and isinstance(index,str)==False:
            raise IndexError('Index must be integer or string name')
        if isinstance(index,int):
            if index>=self.nbuckets:
                raise IndexError('Index is too large')
            name = self.bucketnames[index]
        else:
            name = index
        delattr(self,name)
        self._bucketnames.remove(name)
        
    @property
    def data(self):
        if self.nbuckets>0:
            return self[0].data
        else:
            return None

    @property
    def meta(self):
        if self.nbuckets>0:
            return self[0].meta
        else:
            return None
        
    def __array__(self):
        return self.data

    @property
    def bucketnames(self):
        """ Return the names of all the data buckets """
        # check _bucketnames and add any others in alphabetical order
        names = self._bucketnames.copy()
        nnames = len(names)
        ignore = names+['bucketnames','nbuckets','buckets','data','meta']
        for d in dir(self):
            if d in ignore or d[0]=='_': continue
            if isinstance(getattr(self,d),DataBucket):
                names.append(d)
        if len(names)>nnames:
            self._bucketnames = names  # update
        return names

    @property
    def nbuckets(self):
        return len(self.bucketnames)
    
    @property
    def buckets(self):
        """ Return the names of all the data buckets """
        # We need to check every time since someone might
        # have added a new one
        out = []
        for d in self.bucketnames:
            out.append(getattr(self,d))
        return out

    def index(self,name):
        # Get bucket index by name
        index, = np.where(self.bucketnames == name)
        if len(index)==0:
            return -1
        else:
            return index[0]
        
    def __repr__(self):
        """ Represent the data """
        out = self.__class__.__name__ + '('
        out += str(self.nbuckets) + ' buckets)\n'
        if hasattr(self,'filename') and getattr(self,'filename') is not None:
            out += 'filename = ' + self.filename + '\n'
        #if self.data is not None:
        #    body = np.array2string(self.data, separator=', ', prefix=prefix)
        #else:
        #    body = 'None'
        #out += 'data: ' + body + '\n'
        #out += 'meta: ' + repr(self.meta)
        # Loop over the buckets
        for d in self.buckets:
            out += repr(d)
        return out

    def info(self):
        """ Give more detailed information """
        out = self.__class__.__name__ + '('
        out += str(self.nbuckets) + ' buckets)\n'
        if hasattr(self,'filename') and getattr(self,'filename') is not None:
            out += 'filename = ' + self.filename + '\n'
        print(out)
        for n in self.bucketnames:
            self[n].info()

    def copy(self):
        """ Make a copy """
        return copy.deepcopy(self)
    
    def to_hdulist(self):
        """ Make a HDUList """
        hdu = fits.HDUList()
        hdu.append(fits.PrimaryHDU(self.data,self.header))
        # Get DataBuckets
        for d in self.buckets:
            hdu.append(d.to_hdulist())
        return hdu
    
    @classmethod
    def read(cls,filename):
        """
        Read a file
        """
        if os.path.exists(filename)==False:
            raise FileNotFoundError(filename)
        return DataContainer(filename)
        
    def write(self,filename,overwrite=False):
        """
        Write to a file
        """
        # construct a HDUList object
        hdulist = self.to_hdulist()
        # write to file
        hdulist.writeto(filename,overwrite=overwrite)
