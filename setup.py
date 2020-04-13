from distutils.core import setup, Extension
#from Cython.Build import cythonize
import sys, string, os
import shutil

args = {'name': 'datalogger4',
        'author': 'Arthur Messner',
        'author_email': 'arthur.messner@gmail.com',
        'description': 'Datalogger4',
        'long_description': __doc__,
        'platforms': ['any'],
        'license': 'LGPLv2',
        'packages': ['datalogger4'],
        'version': '4.0.0',
        }

setup(**args)

