#!/usr/bin/env python
# coding: utf8
"""
Merge two CSV files of spending data for 2014 and 2015 years
"""
import sys
import pandas as pd
import csv
import json

def filter_data(arr):
    all = []
    for row in arr:
        d1 = int(row[1])
        d2 = int(row[5])
        if d1 == 228 and d2 == 228:
            print row[2], row[3]
        else:
            all.append(row)
    return all


def run():
    dataset = {}
    a = pd.read_csv("2014.csv")
    b = pd.read_csv("2015.csv")
    merged = a.merge(b, on=['code', 'name'])
    f1 = open('merged.csv', 'w')
    writer = csv.writer(f1)
    writer.writerow(merged.keys())
    data = merged.as_matrix()
    writer.writerows(filter_data(data))




if __name__ == "__main__":
    run()
