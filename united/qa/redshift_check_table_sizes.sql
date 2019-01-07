SELECT   TRIM(pgdb.datname) AS Database,
         TRIM(a.name) AS "Table",
         ((b.mbytes/part.total::decimal)*100)::decimal(5,2) AS pct_of_total,
         b.mbytes,
         b.unsorted_mbytes
FROM     stv_tbl_perm a
JOIN     pg_database AS pgdb
  ON     pgdb.oid = a.db_id
JOIN     ( SELECT   tbl,
                    SUM( DECODE(unsorted, 1, 1, 0)) AS unsorted_mbytes,
                    COUNT(*) AS mbytes
           FROM     stv_blocklist
           GROUP BY tbl ) AS b
       ON a.id = b.tbl
JOIN     ( SELECT SUM(capacity) AS total
           FROM   stv_partitions
           WHERE  part_begin = 0 ) AS part
      ON 1 = 1
WHERE    a.slice = 0
ORDER BY 4 desc, db_id, name;