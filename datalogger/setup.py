from distutils.core import setup, Extension
#from Cython.Build import cythonize
import sys, string, os
import shutil

PACKAGE_NAME = "datalogger"
VERSION = "3.0"

args = {'name': 'datalogger',
        'author': 'Arthur Messner',
        'author_email': 'arthur.messner@gmail.com',
        'description': PACKAGE_NAME,
        'long_description': __doc__,
        'platforms': ['any'],
        'license': 'LGPLv2',
        'packages': [PACKAGE_NAME],
        # Make packages in root dir appear in pywbem module
        'package_dir': {PACKAGE_NAME: ''},
        # Make extensions in root dir appear in pywbem module
        'ext_package': PACKAGE_NAME,
#        'ext_modules' : cythonize('*.pyx'),
        "version" : VERSION
        }

setup(**args)

