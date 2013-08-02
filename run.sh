#!/bin/bash
source ~/libs
for PYFILE in m49.py emdat.py esa.py faosec.py faostat.py hdrstats.py mdg.py unicef.py unterm.py who-athena.py who-athena2.py worldbank-lendinggroups.py worldbank.py
do
    echo $PYFILE
    python $PYFILE > log1/$PYFILE.txt 2> log2/$PYFILE.txt
done
