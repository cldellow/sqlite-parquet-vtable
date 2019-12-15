select rowid % 10, count(*) FROM dataset group by 1 order by 1;
.output
