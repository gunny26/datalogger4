from distutils.core import setup, Extension
#from Cython.Build import cythonize
import sys, string, os
import shutil

args = {'name': 'tilak_datalogger',
        'author': 'Arthur Messner',
        'author_email': 'arthur.messner@tirol-kliniken.at',
        'description': 'Datalogger',
        'long_description': __doc__,
        'platforms': ['any'],
        'license': 'LGPLv2',
        'packages': ['tilak_datalogger'],
        # Make packages in root dir appear in pywbem module
        'package_dir': {'tilak_datalogger': ''},
        # Make extensions in root dir appear in pywbem module
        'ext_package': 'tilak_datalogger',
#        'ext_modules' : cythonize('*.pyx'),
        }

setup(**args)

