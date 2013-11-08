#!/bin/bash
for country in `echo "select region from value where indID='CG060';" | sqlite3 ocha.db`; do echo $country; echo ".mode csv
.headers on
.output csv/$country.csv
select value.*, indicator.name as indNAME from value, indicator where region='$country' and indicator.indID = value.indID;" | sqlite3 ocha.db; done
