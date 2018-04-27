from distutils.core import setup, Extension
#from Cython.Build import cythonize
import sys, string, os
import shutil

args = {'name': 'datalogger3',
        'author': 'Arthur Messner',
        'author_email': 'arthur.messner@gmail.com',
        'description': 'Datalogger3',
        'long_description': __doc__,
        'platforms': ['any'],
        'license': 'LGPLv2',
        'packages': ['datalogger3'],
        'package_dir': {'datalogger3': 'datalogger3'},
        'ext_package': 'datalogger',
        'version' : '3.0'
        }

setup(**args)

