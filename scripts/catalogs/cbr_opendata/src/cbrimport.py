# -*- coding: utf8 -*-

from BeautifulSoup import BeautifulStoneSoup
import csv, os, sys, urllib2
from bsoupxpath import Path
import chardet
import html5lib
from datetime import datetime
from html5lib import treebuilders
from pymongo import Connection

DB_NAME = 'cbr'


EMPTY_SCHEMA = []
VALUES_SCHEMA = [{'name' : 'reg_date', 'type' : 'datetime'}, {'name' : 'val', 'type' : 'float'}]


def import_file(filename, schema, collname, addIndexes=[]):
	i = 0
	c = Connection()
	db = c[DB_NAME]
	db.drop_collection(collname)
	coll = db[collname]
	for k in addIndexes:
		coll.ensure_index(k, ASCENDING, unique=False)
	csvr = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
	for r in csvr:		
		i += 1
		for k in r.keys():
			r[k] = r[k].decode('utf8')
		for k in schema:
			if k['type'] == 'array':
				name = r[k['name']].strip()
				arr = name.split(',') if len(name) > 0 else []
				r[k['name']] = __create_list(arr)
			elif k['type'] == 'datetime':
				r[k['name']] = datetime.strptime(r[k['name']].split('+')[0], '%Y-%m-%dT%H:%M:%S')
			elif k['type'] == 'float':
				r[k['name']] = float(r[k['name']])
		coll.save(r)
		if i % 500 == 0:
			print collname, 'imported', i
	print collname, 'total imported', i


def main():
	import_file('refined/tables.csv', EMPTY_SCHEMA, 'tables')
	import_file('refined/regions.csv', EMPTY_SCHEMA, 'regions')
	import_file('refined/indicators.csv', EMPTY_SCHEMA, 'indicators')
	import_file('refined/values.csv', VALUES_SCHEMA, 'values')
	

if __name__ == "__main__":
	main()	
