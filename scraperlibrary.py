import requests
import urllib.request
import lxml.html
import os
import re
import datetime

from collections import OrderedDict

from bs4 import BeautifulSoup
from bs4.element import (
    CharsetMetaAttributeValue,
    Comment,
    ContentMetaAttributeValue,
    Doctype,
    SoupStrainer
)


####
#
# Souped Up just makes the request and passes you the soup to parse when there is one available.  Just pass the URL. 
#
####
def soupedUp(url):
	theRequest = requests.get(url, allow_redirects=False)
	if theRequest.status_code == 200:
		soup = stripTags(theRequest.text, invalidTags)
	else:
		soup = None
	return soup

####
#
# binarySearach checks through a sequence of numbers to find the last range item in a route.  
# Helpful when you wanat to iterage through to find the last sequence when you don't know it.
#
####
def binarySearch(url, theRange):
	first = 0
	last = len(theRange)-1
	while first <= last:
		middle = (first + last) // 2
		if soupedUp(url+str(middle)) is None:
			last = middle - 1
		else:
			first = middle + 1
	return middle

###
#
# Use this to remove the tags that don't add value ie <br>
#
###
invalidTags = ['br']
def stripTags(html, invalid_tags):
    soup = BeautifulSoup(html, "lxml")

    for tag in soup.findAll(True):
        if tag.name in invalid_tags:
            s = "::"

            for c in tag.contents:
                if not isinstance(c, NavigableString):
                    c = strip_tags(unicode(c), invalid_tags)
                s += unicode(c)

            tag.replaceWith(s)

    return soup


#######
##
## This function cleans the data
##
#######
def cleanHeader(value):
	if value is not None:
		return value.strip().lower().replace('&', 'and').replace('%', 'pct').replace('-', '_').replace('.','').replace(',', '').replace(":", '').replace('(','').replace(')','').replace('/', '').replace('#', 'number').replace(' ', '_')

def cleanData(value, dataType=None):

	thousandsRexEx = re.compile(r'^\d{1,3}(,\d{3})*$') #Search for strings with thousands seperater
	if isinstance(value, str) or value is None:	
		if value is None:
			value = ''
		else:
			value = value.strip().replace('"', '').replace('(', '').replace(')', '')
		if value.startswith('$'):
			value = value.replace('$', '').replace(',','')
		if thousandsRexEx.search(value):
			value = value.replace(',','')
		if value.startswith('http:'):
			value = value.replace('\\','/').replace('//','/').replace('http:/','http://')
		if dataType is not None and (dataType.split('(')[0] == 'INTEGER' or dataType.split('(')[0] == 'DOUBLE' or dataType.split('(')[0] == 'BIGINT'):
			value = value.replace(' 1/4', '.25').replace(' 1/2', '.5').replace(' 3/4', '.75')
			nonText = re.compile(r'[^\d.]+')
			value = nonText.sub("", value)
		value = value.replace("'", r"-").replace("\"", "").replace("\\","").strip()
	return value

###
#
# get_type2 will check the datatype of the data we parsed.  
#
###
register_type = OrderedDict()

register_type["INTEGER"] = {"handle":int, "args": [], "kw": {}}
register_type["DOUBLE"] = {"handle":float, "args": [], "kw": {}}
register_type["DATE"] = {"handle":lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"), "args": [], "kw": {}}
register_type["TEXT"] = {"handle":lambda x: re.match("\w+", x), "args": [], "kw": {}}

scriptName = os.path.basename(__file__).strip('.py')

def get_type2(value):
    type_ = "UNKNOWN"
    for k, v in register_type.items():
        try:
            parsed = v["handle"](value, *v["args"], **v["kw"])
            type_ = k
            break
        except ValueError as E:
            continue
    return  type_

