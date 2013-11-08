#!/bin/bash
for country in `echo "select region from value where indID='CG060';" | sqlite3 ocha.db`; do echo ".mode csv
.headers on
.output csv/$country.csv
select * from value where region='$country';" | sqlite3 ocha.db; done
