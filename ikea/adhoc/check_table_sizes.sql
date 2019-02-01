select   trim(pgdb.datname) as database,
         trim(a.name) as "table",
         ((b.mbytes/part.total::decimal)*100)::decimal(5,2) as pct_of_total,
         b.mbytes,
         b.unsorted_mbytes
from     stv_tbl_perm a
join     pg_database as pgdb
  on     pgdb.oid = a.db_id
join     ( select   tbl,
                    sum( decode(unsorted, 1, 1, 0)) as unsorted_mbytes,
                    count(*) as mbytes
           from     stv_blocklist
           group by tbl ) as b
       on a.id = b.tbl
join     ( select sum(capacity) as total
           from   stv_partitions
           where  part_begin = 0 ) as part
      on 1 = 1
where    a.slice = 0
order by 4 desc, db_id, name;