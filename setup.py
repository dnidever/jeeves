import os
import sys
import shutil
from setuptools import setup, find_packages, find_namespace_packages
from setuptools.command.install import install

setup(name='jeeves',
      version='1.0.0',
      description='Data Butler',
      author='David Nidever',
      author_email='dnidever@montana.edu',
      url='https://github.com/dnidever/jeeves',
      requires=['numpy','astropy(>=4.0)','scipy'],
      zip_safe = False,
      include_package_data=True,
      scripts=['bin/jeeves'],
      packages=find_namespace_packages(where="python"),
      package_dir={"": "python"}      
)
