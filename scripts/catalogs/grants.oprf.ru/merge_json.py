#!/usr/bin/env python
import sys, os, json, os.path

def merge_files(filedir, outfilename):
    filenames = os.listdir(filedir)
    alldata = []
    for name in filenames:
        fname, ext = os.path.splitext(name)
        ext = ext.lower()
        if ext != '.json': continue
        data = json.loads(open(os.path.join(filedir, name), 'r').read())
        if data.has_key('rows'):
            for r in data['rows']:
                r['filename'] = fname
                alldata.append(r)
    f = open(outfilename, 'w')
    f.write(json.dumps({'rows': alldata}, indent=4))
    f.close()

if __name__ == "__main__":
    merge_files(sys.argv[1], sys.argv[2])
