from distutils.core import setup, Extension
#from Cython.Build import cythonize
import sys, string, os
import shutil

args = {'name': 'datalogger',
        'author': 'Arthur Messner',
        'author_email': 'arthur.messner@gmail.com',
        'description': 'Datalogger',
        'long_description': __doc__,
        'platforms': ['any'],
        'license': 'LGPLv2',
        'packages': ['datalogger'],
        # Make packages in root dir appear in pywbem module
        'package_dir': {'datalogger': 'datalogger'},
        # Make extensions in root dir appear in pywbem module
        'ext_package': 'datalogger',
#        'ext_modules' : cythonize('*.pyx'),
        }

setup(**args)

