scripts/cbrexporter.exe regions > raw/regions.xml
scripts/cbrexporter.exe tables > raw/tables.xml
python cbrconvert.py regions
python cbrconvert.py tables
call scripts/get_indicators.bat
python cbrconvert.py indicators
python cbrextract.py
python cbconvert.py values
python cbconvert.py mergevalues

