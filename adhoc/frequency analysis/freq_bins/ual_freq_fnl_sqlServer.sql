
create table  master.dbo.ual_freq_fnl
(
wk          varchar(1000)            not null,
imp_cnt     int            not null,
imps        int            not null,
user_cnt    int            not null,
cost        decimal(20,10)     not null,
con         int            not null,
adj_rev     decimal(20,10)     not null

);



select
  wk,
--  grp_nbr,
  imp_cnt,
  min(cst_pct_run) over (partition by wk, imp_grp) as min_cst
--  rev_pct_run

from (

select
  wk,
  imp_cnt,

--  case when sum((cast(rev_run as decimal(20,10)) / cast(rev_tot as decimal(20,10))) * cast(100 as decimal(20,10))) >= 85
-- --      and sum((cast(cst_run as decimal(20,10)) / cast(cst_tot as decimal(20,10))) * cast(100 as decimal(20,10))) <= 75
--    then 1
--  when sum((cast(rev_run as decimal(20,10)) / cast(rev_tot as decimal(20,10))) * cast(100 as decimal(20,10))) >= 90
-- --      and sum((cast(cst_run as decimal(20,10)) / cast(cst_tot as decimal(20,10))) * cast(100 as decimal(20,10))) <= 75
--    then 2
--  when sum((cast(rev_run as decimal(20,10)) / cast(rev_tot as decimal(20,10))) * cast(100 as decimal(20,10))) >= 95
-- --      and sum((cast(cst_run as decimal(20,10)) / cast(cst_tot as decimal(20,10))) * cast(100 as decimal(20,10))) <= 75
--    then 3
--
--  else 4 end as rnk,

--  sum(usr)                                                                                               as usr,
--  sum((cast(usr as decimal(20,10)) / cast(usr_tot as decimal(20,10))) * cast(100 as decimal(20,10)))     as usr_pct,
  sum(usr_run)                                                                                           as usr_run,
--  sum(usr_tot)                                                                                           as usr_tot,

  sum(case when usr_tot = 0 then 0 else (cast(usr_run as decimal(20,10)) / cast(usr_tot as decimal(20,10))) * cast(100 as decimal(20,10)) end) as usr_pct_run,

--  sum(imp)                                                                                               as imp,
--  sum((cast(imp as decimal(20,10)) / cast(imp_tot as decimal(20,10))) * cast(100 as decimal(20,10)))     as imp_pct,
  sum(imp_run)                                                                                           as imp_run,
  sum(case when imp_tot = 0 then 0 else (cast(imp_run as decimal(20,10)) / cast(imp_tot as decimal(20,10))) * cast(100 as decimal(20,10)) end) as imp_pct_run,

--  sum(cst)                                                                                               as cst,
--  sum((cast(cst as decimal(20,10)) / cast(cst_tot as decimal(20,10))) * cast(100 as decimal(20,10)))     as cst_pct,
  sum(cst_run)                                                                                           as cst_run,
  sum(case when cst_tot = 0 then 0 else (cast(cst_run as decimal(20,10)) / cast(cst_tot as decimal(20,10))) * cast(100 as decimal(20,10)) end) as cst_pct_run,

--  sum(con)                                                                                               as con,
--  sum((cast(con as decimal(20,10)) / cast(con_tot as decimal(20,10))) * cast(100 as decimal(20,10)))     as con_pct,
  sum(con_run)                                                                                           as con_run,
  sum(case when con_tot = 0 then 0 else (cast(con_run as decimal(20,10)) / cast(con_tot as decimal(20,10))) * cast(100 as decimal(20,10)) end) as con_pct_run,

--  sum(rev)                                                                                               as rev,
--  sum((cast(rev as decimal(20,10)) / cast(rev_tot as decimal(20,10))) * cast(100 as decimal(20,10)))     as rev_pct,
  sum(rev_run)                                                                                           as rev_run,
  sum(case when rev_tot = 0 then 0 else (cast(rev_run as decimal(20,10)) / cast(rev_tot as decimal(20,10))) * cast(100 as decimal(20,10)) end) as rev_pct_run

--  row_number() over( order by wk, imp_cnt) as row_num

from (
       select
         wk,
         imp_cnt,
--  sum(user_cnt) as user_cnt,
--  Users
         sum(user_cnt) over (partition by wk)          as usr_tot,
         sum(user_cnt) over (partition by wk
           order by imp_cnt)                         as usr_run,
         sum(user_cnt) over (partition by wk,imp_cnt) as usr,
--  Impressions
         sum(imps) over (partition by wk)             as imp_tot,
         sum(imps) over (partition by wk
           order by imp_cnt)                        as imp_run,
         sum(imps) over (partition by wk,imp_cnt)     as imp,
--  Cost
         sum(cost) over (partition by wk)             as cst_tot,
         sum(cost) over (partition by wk
           order by imp_cnt)                        as cst_run,
         sum(cost) over (partition by wk,imp_cnt)     as cst,
--  Conversions
         sum(con) over (partition by wk)              as con_tot,
         sum(con) over (partition by wk
           order by imp_cnt)                        as con_run,
         sum(con) over (partition by wk,imp_cnt)      as con,
--  Revenue
         sum(adj_rev) over (partition by wk)          as rev_tot,
         sum(adj_rev) over (partition by wk
           order by imp_cnt)                        as rev_run,
         sum(adj_rev) over (partition by wk,imp_cnt)  as rev

       from master.dbo.ual_freq_fnl
     ) as t1

--  where imp_cnt between 27 and 39
group by
  wk,
  imp_cnt

  ) as t2


-- where rev_pct_run >= 90

group by
  imp_cnt,
--  imp_grp,
  wk
--  rev_pct_run

-- order by
--    grp_nbr
