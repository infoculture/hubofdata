#!/bin/sh
CBR=scripts/cbrexporter.exe 
#mono $CBR tables > raw/tables.xml
#mono $CBR regions > raw/regions.xml
#python cbrconvert.py regions
#python cbrconvert.py tables
#call scripts/get_indicators.bat
#python cbrconvert.py indicators
#python cbrextract.py
python cbrconvert.py values
python cbrconvert.py mergevalues