# -*- coding: utf-8 -*-

import sys, os, urllib2, csv

from pymongo import Connection
from BeautifulSoup import BeautifulSoup
from decimal import Decimal
from pyparsing import Word, nums, alphas, oneOf, lineStart, lineEnd, Optional, restOfLine, Literal, ParseException, CaselessLiteral
import re, os.path, cPickle, os
import calendar
import xlrd

MONGO_SERVER = '127.0.0.1'
MONGO_PORT = 27040

def get_reg(code):
	c = Connection(MONGO_SERVER, MONGO_PORT)
	db = c['perepis2002']
	collname = 'tom14_25_sh_0'
	coll = db[collname]
	all = coll.find({'subjectCode': code}).sort('reg_share', -1)
	for o in all:
		if o['reg_share'] > 0 and o['nationkey'] != '':
			print o['reg_share'], o['nationkey'], o['nation']


def main():
#	convert()
#	extract()
	get_reg(sys.argv[1])

if __name__ == '__main__':
	main()