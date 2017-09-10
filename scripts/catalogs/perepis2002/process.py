#!/usr/bin/env python
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
MONGO_PORT = 27017


def convert():
	files = os.listdir('raw')
	for name in files:		
		book = xlrd.open_workbook('raw/%s' %(name), encoding_override="cp1251")	
		print name, book.nsheets
		for shid in range(0, book.nsheets):
			sheet = book.sheet_by_index(shid)
			if sheet.ncols == 0: continue
			f = open('csv/%s_sh_%d.csv' %(name.split('.')[0], shid), 'w')			
			for r in range(0, sheet.nrows):
				record = []
				for c in range(0, sheet.ncols):
					v = sheet.cell_value(r, c)
					if type(v) != type(u''):
						v = str(v)
					else:
						v = v.encode('utf8')
					record.append(v)
				f.write('\t'.join(record) + '\n')
			f.close()			
	pass


def extract_nations():
	skipn = 4
	filename = 'csv/TOM_14_25_sh_0.csv'
	csvr = csv.reader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
	i = 0
	fn = open('dicts/nationality.csv', 'w')
	fn.write('\t'.join(['name', 'total']) +'\n')
	f = open('dicts/regions.csv', 'w')
	f.write('\t'.join(['name',]) +'\n')
	for l in csvr:
		i += 1
		if i < skipn: continue		
		if i == 4:
			for k in l: 
				k = k.strip()
				if len(k) > 0: 
					f.write('%s\n' %(k))
		if i > 4:
			fn.write('\t'.join(l[0:2]).replace('.', ',') + '\n')
		f.close()
	fn.close()
	pass

def extract():
	extract_nations()


def get_dicts():
	nat = {}
	regs = {}
	csvr = csv.DictReader(open('dicts/nationality_refined.csv', 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
	i = 0
	for r in csvr:		
		i += 1
		for k in r.keys():
			r[k] = r[k].decode('cp1251')
		nat[r['name']] = r
	csvr = csv.DictReader(open('dicts/regions_refined.csv', 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
	i = 0
	for reg in csvr:		
		i += 1
		for k in reg.keys():
			reg[k] = reg[k].decode('cp1251')
		regs[reg['name']] = reg
	return nat, regs
		
	
	

def process_nations():
	c = Connection(MONGO_SERVER, MONGO_PORT)
	db = c['perepis2002']
	collname = 'tom14_25_sh_0'
	db.drop_collection(collname)
	coll = db[collname]
	nat, regs = get_dicts()
	skipn = 4
	filename = 'csv/TOM_14_25_sh_0.csv'
	csvr = csv.reader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
	i = 0
	regions = {}
	regvals = {}
	f = file('processed/nations.tsv', 'w')
	keys = ['nationkey', 'nation', 'regionkey', 'region', 'value']
	f.write('\t'.join(keys) +'\n')
	for l in csvr:
		i += 1
		if i < skipn: continue		
		if i == 4:
			for k in range(2, len(l)): 				
				regions[k] = l[k].strip().decode('utf8')
		if i == 5:
			for k in range(2, len(l)): 												
				regvals[regions[k]] = int(l[k].replace('.', ',').split(',')[0]) if l[k] != '-' else 0
		if i > 4:
			l[0] = l[0].decode('utf8', 'ignore')				
			for k in range(2, len(l)): 												
				regv = ''
				if regs.has_key(regions[k]):
					if regs[regions[k]]['rtype'] == 'region':
						regv = str(regs[regions[k]]['subjectCode'])
						rtype = 'region'
					else:		
						regv = str(regs[regions[k]]['districtKey'])
						rtype = 'feddistrict'
#				record = [nat[l[0]]['key'].encode('utf8') if nat.has_key(l[0]) else '', l[0].encode('utf8'), regv, regions[k].encode('utf8'), str(int(l[k].replace('.', ',').split(',')[0])) if l[k] != '-' else "0"]
				record = {'nationkey' : nat[l[0]]['key'].encode('utf8') if nat.has_key(l[0]) else '',
						  'nation' : l[0], 'region' : regions[k], 'value' : int(l[k].replace('.', ',').split(',')[0]) if l[k] != '-' else 0,
						  'rtype' : rtype, 'total' : int(l[1].replace('.', ',').split(',')[0]) if l[k] != '-' else 0}
				if rtype == 'feddistrict':
					record['districtKey'] = regv
				else:
					record['subjectCode'] = regv
				record['nat_share'] = float(record['value']) * 100.0/ record['total'] if record['total'] > 0 else 0
				record['reg_share'] = float(record['value']) * 100.0/ regvals[record['region']] if regvals[record['region']]  > 0 else 0
#				for k in keys
				coll.save(record)
	all = []
	for o in coll.find():
		
		all.append(o)



	pass

def process():
	process_nations()


def main():
#	convert()
#	extract()
	process()

if __name__ == '__main__':
	main()