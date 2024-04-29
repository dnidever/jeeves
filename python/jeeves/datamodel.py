import os
import numpy as np
import yaml

# Jeeves datamodel

class DataModel(object):

    def __init__(self,init):
        self.init

    def copy(self):
        """ Make a copy """
        return copy.deepcopy(self)

    @classmethod
    def read(cls,filename):
        """
        Read from a fits file
        """
        if os.path.exists(filename)==False:
            raise FileNotFoundError(filename)
        # Read from yaml file
        with open(filename, 'r') as f:
            prime_service = yaml.safe_load(f)
        return prime_service
            
    def write(self,filename,overwrite=False):
        """
        Write to a yaml file
        """
        with open(filename,'w') as f:
            yaml.dump(self.data,f)

# Create one from a file
            
# -file type name
# -specify the path or use autopath
# -specify the schema/layout of the file, data buckets
#   data type
