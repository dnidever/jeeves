import os
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
from astropy.table import Table

# Data containers

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
            if k not in ['HISTORY','COMMENT','SIMPLE','EXTEND']:
                setattr(self,k.lower(),v)

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
        head = fits.header.copy()  # start with initial header
        # Delete all history lines
        try:
            del head['HISTORY']
        except:
            pass
        # Update all key/value pairs
        keystoignore = ['wcs','to_header','header','history','istable']
        for d in dir(self):
            if d[0] != '_' and d not in keystoignore:
                head[d] = getattr(self,d)
        # wcs to header
        whead = self.wcs.to_header()
        head += whead
        # Add history
        histhead = self.history.to_header()
        head += histhead
        return head
    
class DataBucket(object):
    """ Container for data and metadata/header """
    
    def __init__(self,data=None,header=None,name=None):
        self.data = data
        self.meta = MetaData(header)
        self.name = name
        # Is this a table or image data
        self.datatype = 'None'
        if data is not None:
            if isinstance(data,Table) or isinstance(data,fits.fitsrec.FITS_rec):
                self.datatype = 'table'
            elif isinstance(data,np.ndarray):
                if data.dtype.names is None:
                    self.datatype = 'ndarray'
                else:
                    self.datatype = 'table'
            else:
                datatype = str(data.__class__)
                datatype = datatype.replace("<class '","")
                datatype = datatype.replace("'>","")                
                self.datatype = datatype
                
    def __repr__(self):
        """ Represent the object """
        prefix = self.__class__.__name__ + '('
        if self.data is not None:
            body = np.array2string(self.data, separator=', ', prefix=prefix)
        else:
            bodu = 'None'
        out = ''.join([prefix, body, ')']) +'\n'
        return out  

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
    
class DataContainer(object):

    def __init__(self,init):

        # how about using the asdf format directly?
        
        # Input types
        # Filename
        if isinstance(init,str):
            hdulist = fits.open(init)
            self._from_hdulist(hdulist)
            hdulist.close()
            self.filename = init
        # HDUList
        elif isinstance(init,fits.HDUList):
            self._from_hdulist(init)
        # Some data
        else:
            self.data = init
            self.meta = MetaData()

    def _from_hdulist(self,hdulist):
        """ Create DataContainer from HDUList """
        self.data = None
        self.meta = None
        for i in range(len(hdulist)):
            # Primary HDU is special            
            if i==0:
                self.data = hdulist[i].data
                self.meta = MetaData(hdulist[i].header)
            # Extensions
            else:
                name = hdulist[i].header.get('extname')
                if name is None:
                    name = 'exten'+str(i)
                db = DataBucket(hdulist[i].data,hdulist[i].header,name)
                setattr(self,name,db)

    def getbucketnames(self):
        """ Return the names of all the data buckets """
        # We need to check every time since someone might
        # have added a new one
        names = []
        for d in dir(self):
            if isinstance(getattr(self,d),DataBucket):
                names.append(d)
        return names

    def getbuckets(self):
        """ Return the names of all the data buckets """
        # We need to check every time since someone might
        # have added a new one
        out = []
        for d in dir(self):
            if isinstance(getattr(self,d),DataBucket):
                out.append(getattr(self,d))
        return out
    
    def __repr__(self):
        """ Represent the data """
        out = self.__class__.__name__ + '\n'
        if self.data is not None:
            body = np.array2string(self.data, separator=', ', prefix=prefix)
        else:
            body = 'None'
        out += 'data = ' + body + '\n'
        out += 'meta = ' + self.meta.__repr__()
        # Loop over the buckets
        for d in self.getbuckets():
            out += d.___repr__()
        return out

    def info(self):
        """ Give more detailed information """
        pass
    
    def to_hdulist(self):
        """ Make a HDUList """
        hdu = fits.HDUList()
        hdu.append(fits.PrimaryHDU(self.data,self.header))
        # Get DataBuckets
        for d in self.getbuckets():
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
