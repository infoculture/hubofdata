# -*- coding: utf8 -*-

from BeautifulSoup import BeautifulStoneSoup
import csv, os, sys
from bsoupxpath import Path
from StringIO import StringIO

def convert_tables():
    f = open('raw/tables.xml', 'r')
    data = f.read()
    f.close()
    soup = BeautifulStoneSoup(data)
    tp = Path('//regtables')
    objs = tp.apply(soup)
    f = open('refined/tables.csv', 'w')
    f.write('\t'.join(['ind_table', 'tbl_name', 'cont']) + '\n')
    for o in objs:
        rname = unicode(o.find('tbl_name').string).encode('utf8', 'ignore')
        f.write('\t'.join([str(o.find('ind_table').string), rname, str(o.find('cont').string)]) + '\n')


def convert_regions():
    f = open('raw/regions.xml', 'r')
    data = f.read()
    f.close()
    soup = BeautifulStoneSoup(data)
    tp = Path('//regions')
    objs = tp.apply(soup)
    f = open('refined/regions.csv', 'w')
    f.write('\t'.join(['reg_id', 'name']) + '\n')
    for o in objs:
        rname = unicode(o.find('regname').string).encode('utf8', 'ignore')
        f.write('\t'.join([str(o.find('reg_id').string), rname]) + '\n')

def convert_inds():
    filename = 'refined/tables.csv'
    csvr = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
    f = open('refined/indicators.csv', 'w')
    f.write('\t'.join(['ind_id', 'ind_name', 'ind_izm', 'ind_table']) + '\n')
    for r in csvr:
        tkey = r['ind_table']
        filename = tkey + '.xml'
        f1 = open('raw/indicators/%s' % filename, 'r')
        data = f1.read()
        f1.close()
        soup = BeautifulStoneSoup(data)
        tp = Path('//indlist')
        objs = tp.apply(soup)
        print tkey
        for o in objs:
            rname = unicode(o.find('ind_name').string).encode('utf8', 'ignore')
            izm = unicode(o.find('ind_izm').string).encode('utf8', 'ignore')
            f.write('\t'.join([str(o.find('ind_id').string), rname, izm, tkey]) + '\n')
    f.close()

def convert_values():
    filename = 'refined/tables.csv'
    csvr = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
    filename = 'refined/regions.csv'
    regs = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
    tables = []
    for o in csvr:
        tables.append(o)
    inds = []
    filename = 'refined/indicators.csv'
    for o in csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE):
        inds.append(o)
#	f = open('refined/values.csv', 'w')
#	f.write('\t'.join(['reg_date', 'reg_id', 'ind_id', 'val']) + '\n')
    for r in regs:
        rkey = r['reg_id']
        if not os.path.exists('refined/values/%s/' % rkey):
            os.makedirs('refined/values/%s' %(rkey))
        for  t in tables:
            tkey = t['ind_table']
            ires = []
            for i in inds:
                if i['ind_table'] == tkey:
                    ires.append(i['ind_id'])
            filepath = 'raw/values/%s/%s__%s.xml' %(rkey, rkey, '_'.join(ires))
            if not os.path.exists(filepath): continue
            f1 = open(filepath, 'r')
            data = f1.read()
            f1.close()
            filepath = 'refined/values/%s/%s__%s.csv' %(rkey, rkey, '_'.join(ires))
            if os.path.exists(filepath):
                print 'Skipped', filepath
                continue
            f = open(filepath, 'w')
            f.write('\t'.join(['reg_date', 'reg_id', 'ind_id', 'val']) + '\n')
            soup = BeautifulStoneSoup(data)
            tp = Path('//reg_val')
            objs = tp.apply(soup)
            for o in objs:
                keys = ['regdate', 'reg_id', 'ind_id', 'val']
                vals = []
                for k in keys:
                    vals.append(str(o.find(k).string))
                f.write('\t'.join(vals) + '\n')
            print tkey, filepath

            f.close()


def merge_values():
    filename = 'refined/tables.csv'
    csvr = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
    filename = 'refined/regions.csv'
    regs = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
    tables = []
    for o in csvr:
        tables.append(o)
    inds = []
    filedata = {}
    keys = ['table_id', 'table_name', 'reg_id', 'reg_name', 'ind_id', 'ind_name', 'reg_date', 'val']
    filename = 'refined/indicators.csv'
    for o in csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE):
        inds.append(o)
        filedata[o['ind_id']] = StringIO()
        filedata[o['ind_id']].write('\t'.join(keys) + '\n')


#    keys = ['reg_date', 'reg_id', 'ind_id', 'val']
#    f = open('refined/values.csv', 'w')
    for r in regs:
        rkey = r['reg_id']
        for  t in tables:
            tkey = t['ind_table']
            ires = []
            for i in inds:
                if i['ind_table'] == tkey:
                    ires.append(i['ind_id'])
            filepath = 'refined/values/%s/%s__%s.csv' %(rkey, rkey, '_'.join(ires))
            if not os.path.exists(filepath): continue
            vres = csv.DictReader(open(filepath, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
            for v in vres:
                vals = []
                for k in keys:
                    vals.append(v[k])
                f.write('\t'.join(vals) + '\n')
            print tkey, filepath

    f.close()


def split_values():
    filename = 'refined/tables.csv'
    csvr = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
    filename = 'refined/regions.csv'
    regsvr = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
    regions = {}
    for o in regsvr:
        regions[o['reg_id']] = o['name']
    tables = {}
    for o in csvr:
        tables[o['ind_table']] = o['tbl_name']
    inds = {}
    filedata = {}
    keys = ['table_id', 'table_name', 'reg_id', 'reg_name', 'ind_id', 'ind_name', 'reg_date', 'val']
    filename = 'refined/indicators.csv'
    for o in csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE):
        inds[o['ind_id']] = o
        filedata[o['ind_id']] = StringIO()
        filedata[o['ind_id']].write('\t'.join(keys) + '\n')
    values = csv.DictReader(open('refined/values.csv', 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)

    for v in values:
        break
        ind = inds[v['ind_id']]
        rkey = v['reg_id']
        vals = [ind['ind_table'], tables[ind['ind_table']], v['reg_id'], regions[v['reg_id']], v['ind_id'], ind['ind_name'], v['reg_date'], v['val']]
        s = '\t'.join(vals) + '\n'
        filedata[v['ind_id']].write(s)

    for k, v in filedata.items():
        break
        f = open('refined/indvalues/inddata_%s.csv' % k, 'w')
        f.write(v.getvalue())
        f.close()

    keys = ['table_id', 'table_name', 'ind_id', 'ind_name']
    f = open('refined/fullinds.csv', 'w')
    f.write('\t'.join(keys) + '\n')
    for k, ind in inds.items():
        print ind
        if ind['ind_table'] is None: continue
        vals = [ind['ind_table'], tables[ind['ind_table']], k, ind['ind_name']]
        s = '\t'.join(vals) + '\n'
        f.write(s)
    f.close()


def usage():
        print """cbrconvert.py - converter of Russian Centrobank XML data
Usage:
    cbrconvert.py regions - converts regions XML
    cbrconvert.py tables - converts tables XML
    cbrconvert.py indicators - converts indicators
    cbrconvert.py values - converts values of indicators
    cbrconvert.py mergevalues - merges all values into one CSV file
    cbrconvert.py splitvalues - split all values into indicators CSV files

"""


def main():
    if len(sys.argv) > 1:
        key = sys.argv[1]
        if key == 'regions':
            convert_regions()
        elif key == 'tables':
            convert_tables()
        elif key == 'indicators':
            convert_inds()
        elif key == 'values':
            convert_values()
        elif key == 'mergevalues':
            merge_values()
        elif key == 'splitvalues':
            split_values()
        else:
            usage()
    else:
        usage()


if __name__ == "__main__":
    main()

