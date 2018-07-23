





select
    cat,
--  placement,
    sum(trans) as tickets,
    sum(vew_rev) as vew_rev,
    sum(clk_rev) as clk_rev,
    sum(rev) as rev,
--     sum( u1) as u1,
                    count(b.user_id) as users,
                    count(distinct b.user_id) as unique_users
--                     sum(trans)/count(b.user_id) as tickets_per_user,
--                     sum(trans)/count(distinct b.user_id) as tickets_per_u_user
                -- cvr_nbr,
                -- imp_nbr
from
    (select
                                user_id,
--      p1.placement,
                                -- t1.site_id_dcm,
--                                 interaction_time                            as conversiontime,
                                case
                                when regexp_instr(p1.placement, '.*BusinessClassTicket.*' ) > 0  then 'Biz'
                                when regexp_instr(p1.placement, '.*EconomyNoBidStrategy.*' ) > 0 then 'Econ'
                                when regexp_instr(p1.placement, '.*FirstClass.*' ) > 0           then 'First'
                                when regexp_instr(p1.placement, '.*CatchAll1.*' ) > 0            then 'CatchAll1'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy1.*') > 0 and regexp_instr(p1.placement, '.*General.*')   > 0 then 'Test-Bid1-Member'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy1.*') > 0 and regexp_instr(p1.placement, '.*NonMember.*') > 0 then 'Test-Bid1-NonMember'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy2.*') > 0 and regexp_instr(p1.placement, '.*General.*')   > 0 then 'Test-Bid2-Member'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy2.*') > 0 and regexp_instr(p1.placement, '.*NonMember.*') > 0 then 'Test-Bid2-NonMember'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy3.*') > 0 and regexp_instr(p1.placement, '.*General.*')   > 0 then 'Test-Bid3-Member'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy3.*') > 0 and regexp_instr(p1.placement, '.*NonMember.*') > 0 then 'Test-Bid3-NonMember'
                                when regexp_instr(p1.placement, '.*Control.*') > 0  and regexp_instr(p1.placement, '.*BidStrategy1.*') > 0 then 'Control-Bid1'
                                when regexp_instr(p1.placement, '.*Control.*') > 0  and regexp_instr(p1.placement, '.*BidStrategy2.*') > 0 then 'Control-Bid2'
                                when regexp_instr(p1.placement, '.*Control.*') > 0  and regexp_instr(p1.placement, '.*BidStrategy3.*') > 0 then 'Control-Bid3'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*First.*') > 0 and regexp_instr(p1.placement, '.*Business.*') > 0 then 'Test-First/Biz'
                                when regexp_instr(p1.placement, '.*Control.*') > 0  and regexp_instr(p1.placement, '.*First.*') > 0 and regexp_instr(p1.placement, '.*Business.*') > 0 then 'Control-First/Biz'
-- when site_id_dcm='1853562' and (p1.placement  like '.*Control.*')  then 'Co > 0ntrol'
-- when site_id_dcm='1853562' and (p1.placement  like '.*Test.*')  then 'Test'
else 'Other' end as cat,
        'activity' as tbl,
--         p1.placement,
--         row_number() over (partition by user_id order by user_id) as u1,
                                sum(total_conversions) as trans
                                ,sum(case when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') = 0 and t1.conversion_id = 1 then cast(((t1.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
                                when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') > 0 and t1.conversion_id = 1 then cast(((t1.total_revenue*1000)/.0103) as decimal(20,10)) else 0 end ) as clk_rev
                                ,sum(case when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') = 0 and t1.conversion_id = 2 then cast(((t1.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
                                when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') > 0 and t1.conversion_id = 2 then cast(((t1.total_revenue*1000)/.0103) as decimal(20,10)) else 0 end ) as vew_rev
                                ,sum(case when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') = 0 then cast(((t1.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
                                when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') > 0 then cast(((t1.total_revenue*1000)/.0103) as decimal(20,10)) end) as rev
--                                 row_number() over ()                        as cvr_nbr
--                              count(*) as trans
                from
                                wmprodfeeds.united.dfa2_activity    as t1
                -- these JOINs increase runtime significantly, but we need placement names to filter Sojern's tactics
                left join
                                wmprodfeeds.united.dfa2_placements  as p1
                                on t1.placement_id = p1.placement_id

                left join wmprodfeeds.exchangerates.exchange_rates as rates
                on upper(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3)) = upper(rates.currency)
                and md_event_date_loc = rates.date

                where
                                t1.activity_id = 978826 and
                                t1.campaign_id = 20606595 and
                                user_id != '0' and
                                (t1.site_id_dcm = 1239319 and regexp_instr(placement,'.*_RON_.*') > 0) and

                                md_interaction_date_loc between '2018-01-14' and '2018-01-27' and
                                advertiser_id <> 0 and
                                (length(isnull (event_sub_type,'')) > 0)
                group by user_id,
                    p1.placement

union all

                select
                                user_id,
--                  p1.placement,
                                -- t2.site_id_dcm,
--                                 event_time                                      as impressiontime
                                -- row_number() over ()                            as imp_nbr
                                case
                                when regexp_instr(p1.placement, '.*BusinessClassTicket.*' ) > 0  then 'Biz'
                                when regexp_instr(p1.placement, '.*EconomyNoBidStrategy.*' ) > 0 then 'Econ'
                                when regexp_instr(p1.placement, '.*FirstClass.*' ) > 0           then 'First'
                                when regexp_instr(p1.placement, '.*CatchAll1.*' ) > 0            then 'CatchAll1'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy1.*') > 0 and regexp_instr(p1.placement, '.*General.*')   > 0 then 'Test-Bid1-Member'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy1.*') > 0 and regexp_instr(p1.placement, '.*NonMember.*') > 0 then 'Test-Bid1-NonMember'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy2.*') > 0 and regexp_instr(p1.placement, '.*General.*')   > 0 then 'Test-Bid2-Member'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy2.*') > 0 and regexp_instr(p1.placement, '.*NonMember.*') > 0 then 'Test-Bid2-NonMember'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy3.*') > 0 and regexp_instr(p1.placement, '.*General.*')   > 0 then 'Test-Bid3-Member'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*BidStrategy3.*') > 0 and regexp_instr(p1.placement, '.*NonMember.*') > 0 then 'Test-Bid3-NonMember'
                                when regexp_instr(p1.placement, '.*Control.*') > 0  and regexp_instr(p1.placement, '.*BidStrategy1.*') > 0 then 'Control-Bid1'
                                when regexp_instr(p1.placement, '.*Control.*') > 0  and regexp_instr(p1.placement, '.*BidStrategy2.*') > 0 then 'Control-Bid2'
                                when regexp_instr(p1.placement, '.*Control.*') > 0  and regexp_instr(p1.placement, '.*BidStrategy3.*') > 0 then 'Control-Bid3'
                                when regexp_instr(p1.placement, '.*Test.*'   ) > 0  and regexp_instr(p1.placement, '.*First.*') > 0 and regexp_instr(p1.placement, '.*Business.*') > 0 then 'Test-First/Biz'
                                when regexp_instr(p1.placement, '.*Control.*') > 0  and regexp_instr(p1.placement, '.*First.*') > 0 and regexp_instr(p1.placement, '.*Business.*') > 0 then 'Control-First/Biz'
-- when site_id_dcm='1853562' and (p1.placement  like '.*Control.*')  then 'Co > 0ntrol'
-- when site_id_dcm='1853562' and (p1.placement  like '.*Test.*')  then 'Test'
else 'Other' end as cat,
        'impression' as tbl,
--         p1.placement,
--          count(1) over (partition by user_id) as u1,
                                0 as trans,
                                0 as clk_rev,
                                0 as vew_rev,
                                0 as rev
                from
                                wmprodfeeds.united.dfa2_impression      as t2
                left join
                                wmprodfeeds.united.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                user_id != '0' and
                                (t2.site_id_dcm = 1239319 and regexp_instr(placement,'.*_RON_.*') > 0) and
                                t2.campaign_id = 20606595 and
                                md_event_date_loc between '2018-01-14' and '2018-01-27' and
                                advertiser_id <> 0
        group by user_id,
            p1.placement
                ) as b
-- where
--                 a.user_id = b.user_id and
--                 -- DCM lookback window is set to 7 days, so interaction_time and event_time already match in the log files
--                 conversiontime = impressiontime

-- where cat = 'Other'

group by cat
--  , placement