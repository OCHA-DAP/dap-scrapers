.output minmax
.mode csv
select min(cast(value as float)), max(cast(value as float)), name, value.indID from value join indicator where value.indID = indicator.indID group by value.indID;

