#!/usr/bin/env python
import sys
import base64
import binascii
import logging
import re

#################### hack begin ##########################
#
# hack to mimic some python 2.x behaviour about string representation of tuples
#
def _b64encode_p3(list_obj):
    if len(list_obj) == 1:
        start = "(u'" + list_obj[0] + "',)"
    else:
        start = "(u'" + "', u'".join((str(key) for key in list_obj)) + "')"
    encoded_b = base64.urlsafe_b64encode(start.encode("utf-8"))
    encoded_str = encoded_b.decode("ascii")
    # print("%s -> %s -> %s -> %s" % (list_obj, start, encoded_str,  b64decode(encoded_str)))
    return encoded_str

def _b64encode_p2(list_obj):
    encoded_b = base64.urlsafe_b64encode(unicode(tuple(list_obj)))
    encoded_str = encoded_b.decode("ascii")
    # print("%s -> %s -> %s" % (list_obj, encoded, b64decode(encoded)))
    return encoded_str

def _b64decode(encoded):
    try:
        # encoded must be type str not unicode,
        # otherwise TypeError in b64decodecharacter mapping must return integer, None or unicode
        decoded_b = base64.urlsafe_b64decode(str(encoded))
        decoded_str = decoded_b.decode("utf-8")
        # print("%s -> %s" % (encoded, decoded_str))
        return decoded_str
    except binascii.Error as exc:
        logging.exception(exc)
        logging.error("string %s<%s> could not be base64-decoded", encoded, type(encoded))
        raise exc
    except TypeError as exc:
        logging.exception(exc)
        logging.error("string %s<%s> could not be base64-decoded", encoded, type(encoded))
        raise exc

def b64eval(encoded):
    """
    like
    (u'vsanapp3', u'102', u'0', u'19', u'19', u'Primary Layout', u'114') if multiple fields
    (u'vsanapp3', ) if single field

    parameters:
    encoded <str>

    returns:
    <tuple>
    """
    decoded = b64decode(encoded)
    matches = re.findall("'(.*?)'", decoded)
    return tuple(matches)

if sys.version_info < (3, 0):
    print("using python 2 base64 coding functions")
    b64encode = _b64encode_p2
    b64decode = _b64decode
else:
    b64encode = _b64encode_p3
    b64decode = _b64decode
##################### hack end ###########################


