import os
import numpy as np
import traceback
import subprocess
from . import datamodel,utils


# Ideas
# -configuration file that specifies the paths and filenames
#   allow the use of code but with special symbols denoting it
#   allow for using a tag that specifies it's a function (like in sdss_access)
# -how about reading/writing?  just to/from FITS?  return hdu?
# -should we save meta-data to a database and allow for it to be searched?
#   I think the LSST butler does this

# figure out how I would use jeeves for my various pipelines
#  delve, nsc, apogee, etc.

# Ideas from PHOTRED/MAPS, SMASH, DELVE, APOGEE/SDSS, NSC, LSST, JWST

#PROJECT = None
#PROJECT_DIRECTORY = None

#def set_project(name):
#    """ Set project name for this Python session."""
#    global PROJECT
#    PROJECT = name

def init_project(name,directory):
    """
    Initialize a new project
    """

    # Make the directory
    try:
        os.makedirs(directory)
    except:
        print('Making '+str(directory)+' failed')
        traceback.print_exc()

    # Create sub-directories
    for n in ['config','data','registry']:
        os.makedirs(os.path.join(directory,n))

    # config/
    #   main configuration file and the individual
    #   datamodel files
    # data/
    #   where the data lives
    #   a subdirectory for each file type
    # registry/
    #   the databases, one per file type
        
    # Create the configuration file
    # yaml file
    config = {'name':name,'directory':directory}
    configfile = os.path.join(directory,'config','config.yaml')
    write_config(config,configfile)

    # Add project to ~/.jeeves/projects file
    homedir = os.path.expanduser("~")
    jdir = os.path.join(homedir,'.jeeves')
    if os.path.exists(jdir)==False:
        os.makedirs(jdir)
    # Create project file
    projectsfile = os.path.join(jdir,'projects')
    if os.path.exists(projectsfile)==False:
        projects = {}
    else:
        projects = read_config(projectsfile)
    # Add new project
    projects[name] = {'name':name,'directory':directory}
    write_config(projects,projectsfile)
    
def add_datamodel(name,dmodel,project=None):
    """
    Add file type/datamodel
    """
    
    if isinstance(name,str)==False:
        raise ValueError('name must be a string')
    if isinstance(dmodel,datamodel.DataModel)==False:
        raise ValueError('dmodel must be a DataModel object')

    # Project name
    if project is None and PROJECT is None:
        raise ValueError('Need project name')
    if project is None:
        project = PROJECT

    # Load global jeeves projects file
    pconfig = read_config(projects_filename())
    if project not in pconfig.keys():
        raise ValueError(str(project)+' not found')
    # Get directory
    directory = pconfig[name]['directory']
    # Add to projectconfig file
    configfile = os.path.join(directory,'config','config.yaml')
    config = read_config(configfile)
    if 'datamodels' in config.keys():
        config['datamodels'].append(name)
    else:
        config['datamodels'] = [name]
    write_config(config,configfile)
    # Add own config file
    dconfigfile = os.path.join(directory,'config',name+'.yaml')
    datamodel.write(dconfigfile)
    # Create data directory
    datadir = os.path.join(directory,'data',name)
    os.makedirs(datadir)
    # create blank database
    dbname = os.path.join(directory,'registry',name+'db')
    res = subprocess.run(['sqlite3',dbname],capture_output=True)
    
class JeevesProject(object):
    """
    Jeeves Project object
    """

    def __init__(self,name):
        self.name = name
        self.__projects_filename = projects_filename()

    def initialize(self):
        init_project(self.name)

    def read(self,kind,key):
        """ Read a file."""
        # Open the registry (if it's not open already
        # perform the query
        registry.query()

    def write(self,data,kind,key):
        """ Write a file ."""
        pass

    def register(self,files):
        """ Add files to the registry."""
        pass

    def exists(self,kind,key):
        """ Check if data exists."""
        pass

    def delete(self,kind,key):
        """ Delete data."""
        pass
