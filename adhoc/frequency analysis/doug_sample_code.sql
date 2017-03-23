
create table tbl1 as
select
                userid,
                conversiontime,
                revenue,
                impressiontime,
                cvr_nbr,
                imp_nbr
from
                (select
                                user_id,
                                revenue,
                                conversiontime,
                                row_number() over() as cvr_nbr
                from
                               dfa_activity) a,
                (select
                                user_id,
                                impressiontime,
                                row_number() over() as imp_nbr
                from
                               dfa_impression) b
where
                a.userid = b.userid and
                conversiontime > impressiontime;

-- ================================================================================

create tbl2 as
select
                userid,
                cvr_nbr,
                1/count(distinct imp_nbr) as cvr_credit
from
                tbl1
group by
                1,2;

-- ================================================================================

create table tbl3 as
select
                a.*,
                date_part('year',impression_time) as yr,
                date_part('week',impression_time) as wk,
                cvr_credit
from
                tbl1 a,
                tbl2 b
where
                a.userid=b.userid and
                a.cvr_nbr=b.cvr_nbr;

-- ================================================================================

select
                yr,
                wk,
                count(distinct userid) as user_cnt,
                count(distinct imp_nbr) as imp_cnt,
                sum(cvr_credit) as cvr_credit,
                sum(revenue*cvr_credit) as revenue
from
                tbl3
group by
                1,2;
