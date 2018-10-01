create table master.dbo.ual_freq_fnl_2
(
  wk         varchar(500) not null,
  imp_grp    int          not null,
  imps       int,
  imps_1     int,
  imps_2     int,
  imps_3     int,
  imps_0     int,
  user_cnt   int,
  user_cnt_1 int,
  user_cnt_2 int,
  user_cnt_3 int,
  user_cnt_0 int,
  cst        decimal(20,10),
  cst_1      decimal(20,10),
  cst_2      decimal(20,10),
  cst_3      decimal(20,10),
  cst_0      decimal(20,10),
  con        int,
  con_1      int,
  con_2      int,
  con_3      int,
  con_0      int,
  rev        decimal(20,10),
  rev_1      decimal(20,10),
  rev_2      decimal(20,10),
  rev_3      decimal(20,10),
  rev_0      decimal(20,10)
);





select
imp_grp,
avg(usr_p) as usr_p,
avg(usr_p_1) as usr_p_1,
avg(usr_p_2) as usr_p_2,
avg(usr_p_3) as usr_p_3,
avg(usr_p_0) as usr_p_0,
avg(imp_p) as imp_p,
avg(imp_p_1) as imp_p_1,
avg(imp_p_2) as imp_p_2,
avg(imp_p_3) as imp_p_3,
avg(imp_p_0) as imp_p_0,
avg(cst_p) as cst_p,
avg(cst_p_1) as cst_p_1,
avg(cst_p_2) as cst_p_2,
avg(cst_p_3) as cst_p_3,
avg(cst_p_0) as cst_p_0,
avg(con_p) as con_p,
avg(con_p_1) as con_p_1,
avg(con_p_2) as con_p_2,
avg(con_p_3) as con_p_3,
avg(con_p_0) as con_p_0,
avg(rev_p) as rev_p,
avg(rev_p_1) as rev_p_1,
avg(rev_p_2) as rev_p_2,
avg(rev_p_3) as rev_p_3,
avg(rev_p_0) as rev_p_0
from (

select
  wk,
  imp_grp,

--  sum(usr)                                                                                               as usr,
--  sum((cast(usr as decimal(20,10)) / cast(usr_tot as decimal(20,10))) * cast(100 as decimal(20,10)))     as usr_pct,

--  sum(usr_tot)                                                                                           as usr_tot,
  -- Calculate % of running total/total for week
  sum(usr_run)                                                                                             as usr_r_tot,
  sum((cast(usr_run as decimal(20,10)) / cast(usr_tot as decimal(20,10))) * cast(100 as decimal(20,10)))   as usr_p,
  sum((cast(usr_run_1 as decimal(20,10)) / cast(usr_run as decimal(20,10))) * cast(100 as decimal(20,10))) as usr_p_1,
  sum((cast(usr_run_2 as decimal(20,10)) / cast(usr_run as decimal(20,10))) * cast(100 as decimal(20,10))) as usr_p_2,
  sum((cast(usr_run_3 as decimal(20,10)) / cast(usr_run as decimal(20,10))) * cast(100 as decimal(20,10))) as usr_p_3,
  sum((cast(usr_run_0 as decimal(20,10)) / cast(usr_run as decimal(20,10))) * cast(100 as decimal(20,10))) as usr_p_0,
  sum(imp_run)                                                                                             as imp_r_tot,
  sum((cast(imp_run as decimal(20,10)) / cast(imp_tot as decimal(20,10))) * cast(100 as decimal(20,10)))   as imp_p,
  sum((cast(imp_run_1 as decimal(20,10)) / cast(imp_run as decimal(20,10))) * cast(100 as decimal(20,10))) as imp_p_1,
  sum((cast(imp_run_2 as decimal(20,10)) / cast(imp_run as decimal(20,10))) * cast(100 as decimal(20,10))) as imp_p_2,
  sum((cast(imp_run_3 as decimal(20,10)) / cast(imp_run as decimal(20,10))) * cast(100 as decimal(20,10))) as imp_p_3,
  sum((cast(imp_run_0 as decimal(20,10)) / cast(imp_run as decimal(20,10))) * cast(100 as decimal(20,10))) as imp_p_0,
  sum(cst_run)                                                                                             as cst_r_tot,
  sum((cast(cst_run as decimal(20,10)) / cast(cst_tot as decimal(20,10))) * cast(100 as decimal(20,10)))   as cst_p,
  sum((cast(cst_run_1 as decimal(20,10)) / cast(cst_run as decimal(20,10))) * cast(100 as decimal(20,10))) as cst_p_1,
  sum((cast(cst_run_2 as decimal(20,10)) / cast(cst_run as decimal(20,10))) * cast(100 as decimal(20,10))) as cst_p_2,
  sum((cast(cst_run_3 as decimal(20,10)) / cast(cst_run as decimal(20,10))) * cast(100 as decimal(20,10))) as cst_p_3,
  sum((cast(cst_run_0 as decimal(20,10)) / cast(cst_run as decimal(20,10))) * cast(100 as decimal(20,10))) as cst_p_0,
  sum(con_run)                                                                                             as con_r_tot,
  sum((cast(con_run as decimal(20,10)) / cast(con_tot as decimal(20,10))) * cast(100 as decimal(20,10)))   as con_p,
  sum((cast(con_run_1 as decimal(20,10)) / cast(con_run as decimal(20,10))) * cast(100 as decimal(20,10))) as con_p_1,
  sum((cast(con_run_2 as decimal(20,10)) / cast(con_run as decimal(20,10))) * cast(100 as decimal(20,10))) as con_p_2,
  sum((cast(con_run_3 as decimal(20,10)) / cast(con_run as decimal(20,10))) * cast(100 as decimal(20,10))) as con_p_3,
  sum((cast(con_run_0 as decimal(20,10)) / cast(con_run as decimal(20,10))) * cast(100 as decimal(20,10))) as con_p_0,
  sum(rev_run)                                                                                             as rev_r_tot,
  sum((cast(rev_run as decimal(20,10)) / cast(rev_tot as decimal(20,10))) * cast(100 as decimal(20,10)))   as rev_p,
  sum((cast(rev_run_1 as decimal(20,10)) / cast(rev_run as decimal(20,10))) * cast(100 as decimal(20,10))) as rev_p_1,
  sum((cast(rev_run_2 as decimal(20,10)) / cast(rev_run as decimal(20,10))) * cast(100 as decimal(20,10))) as rev_p_2,
  sum((cast(rev_run_3 as decimal(20,10)) / cast(rev_run as decimal(20,10))) * cast(100 as decimal(20,10))) as rev_p_3,
  sum((cast(rev_run_0 as decimal(20,10)) / cast(rev_run as decimal(20,10))) * cast(100 as decimal(20,10))) as rev_p_0,
--  sum(imp)                                                                                               as imp,
--  sum((cast(imp as decimal(20,10)) / cast(imp_tot as decimal(20,10))) * cast(100 as decimal(20,10)))     as imp_pct,




--  sum(cst)                                                                                               as cst,
--  sum((cast(cst as decimal(20,10)) / cast(cst_tot as decimal(20,10))) * cast(100 as decimal(20,10)))     as cst_pct,


--  sum(con)                                                                                               as con,
--  sum((cast(con as decimal(20,10)) / cast(con_tot as decimal(20,10))) * cast(100 as decimal(20,10)))     as con_pct,


--  sum(rev)                                                                                               as rev,
--  sum((cast(rev as decimal(20,10)) / cast(rev_tot as decimal(20,10))) * cast(100 as decimal(20,10)))     as rev_pct,


  row_number() over( order by wk, imp_grp) as row_num

from (
       select
         wk,
         imp_grp,
--  sum(user_cnt)                                              as user_cnt,
--  Users
         sum(user_cnt)   over (partition by wk)                  as usr_tot,
         sum(user_cnt)   over (partition by wk order by imp_grp) as usr_run,
         sum(user_cnt_1) over (partition by wk order by imp_grp) as usr_run_1,
         sum(user_cnt_2) over (partition by wk order by imp_grp) as usr_run_2,
         sum(user_cnt_3) over (partition by wk order by imp_grp) as usr_run_3,
         sum(user_cnt_0) over (partition by wk order by imp_grp) as usr_run_0,
         sum(user_cnt) over (partition by wk,imp_grp)            as usr,
--  Impressions
         sum(imps)   over (partition by wk)                      as imp_tot,
         sum(imps)   over (partition by wk order by imp_grp)     as imp_run,
         sum(imps_1) over (partition by wk order by imp_grp)     as imp_run_1,
         sum(imps_2) over (partition by wk order by imp_grp)     as imp_run_2,
         sum(imps_3) over (partition by wk order by imp_grp)     as imp_run_3,
         sum(imps_0) over (partition by wk order by imp_grp)     as imp_run_0,
         sum(imps) over (partition by wk,imp_grp)                as imp,
--  Cost
         sum(cst)   over (partition by wk)                       as cst_tot,
         sum(cst)   over (partition by wk order by imp_grp)      as cst_run,
         sum(cst_1) over (partition by wk order by imp_grp)      as cst_run_1,
         sum(cst_2) over (partition by wk order by imp_grp)      as cst_run_2,
         sum(cst_3) over (partition by wk order by imp_grp)      as cst_run_3,
         sum(cst_0) over (partition by wk order by imp_grp)      as cst_run_0,
         sum(cst) over (partition by wk,imp_grp)                 as cst,
--  Conversions
         sum(con)   over (partition by wk)                       as con_tot,
         sum(con)   over (partition by wk order by imp_grp)      as con_run,
         sum(con_1) over (partition by wk order by imp_grp)      as con_run_1,
         sum(con_2) over (partition by wk order by imp_grp)      as con_run_2,
         sum(con_3) over (partition by wk order by imp_grp)      as con_run_3,
         sum(con_0) over (partition by wk order by imp_grp)      as con_run_0,
         sum(con) over (partition by wk,imp_grp)                 as con,
--  Revenue
         sum(rev)   over (partition by wk)                       as rev_tot,
         sum(rev)   over (partition by wk order by imp_grp)      as rev_run,
         sum(rev_1) over (partition by wk order by imp_grp)      as rev_run_1,
         sum(rev_2) over (partition by wk order by imp_grp)      as rev_run_2,
         sum(rev_3) over (partition by wk order by imp_grp)      as rev_run_3,
         sum(rev_0) over (partition by wk order by imp_grp)      as rev_run_0,
         sum(rev) over (partition by wk,imp_grp)                 as rev

       from master.dbo.ual_freq_fnl_2
     ) as t1
group by
  wk,
  imp_grp
  ) as t2
--  where imp_grp in ('0','13','26','39','52')
group by imp_grp

