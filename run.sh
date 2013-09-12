#!/bin/bash
source ~/libs
for PYFILE in m49.py acled.py echo.py emdat.py esa.py faosec.py faostat.py hdr-disaster.py hdrstats.py mdg.py unicef.py unterm.py weather.py who-athena.py who-athena2.py wikipedia.py worldbank-lendinggroups.py worldbank.py worldaerodata.py
do
    echo $PYFILE
    python $PYFILE > log1/$PYFILE.txt 2> log2/$PYFILE.txt
done

# todo: reliefweb-api
# tood: sanity
