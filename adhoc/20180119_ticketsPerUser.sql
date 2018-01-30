





select
    cat,
--  placement,
                    sum(trans) as tickets,
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
                                when regexp_like(p1.placement, '.*BusinessClassTicket.*' ,'ib')  then 'Biz'
                                when regexp_like(p1.placement, '.*EconomyNoBidStrategy.*' ,'ib') then 'Econ'
                                when regexp_like(p1.placement, '.*FirstClass.*' ,'ib')           then 'First'
                                when regexp_like(p1.placement, '.*CatchAll1.*' ,'ib')            then 'CatchAll1'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy1.*','ib') and regexp_like(p1.placement, '.*General.*','ib') then 'Test-Bid1-Member'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy1.*','ib') and regexp_like(p1.placement, '.*NonMember.*','ib') then 'Test-Bid1-NonMember'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy2.*','ib') and regexp_like(p1.placement, '.*General.*','ib') then 'Test-Bid2-Member'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy2.*','ib') and regexp_like(p1.placement, '.*NonMember.*','ib') then 'Test-Bid2-NonMember'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy3.*','ib') and regexp_like(p1.placement, '.*General.*','ib') then 'Test-Bid3-Member'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy3.*','ib') and regexp_like(p1.placement, '.*NonMember.*','ib') then 'Test-Bid3-NonMember'
                                when regexp_like(p1.placement, '.*Control.*','ib')  and regexp_like(p1.placement, '.*BidStrategy1.*','ib') then 'Control-Bid1'
                                when regexp_like(p1.placement, '.*Control.*','ib')  and regexp_like(p1.placement, '.*BidStrategy2.*','ib') then 'Control-Bid2'
                                when regexp_like(p1.placement, '.*Control.*','ib')  and regexp_like(p1.placement, '.*BidStrategy3.*','ib') then 'Control-Bid3'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*First.*','ib') and regexp_like(p1.placement, '.*Business.*','ib') then 'Test-First/Biz'
                                when regexp_like(p1.placement, '.*Control.*','ib')  and regexp_like(p1.placement, '.*First.*','ib') and regexp_like(p1.placement, '.*Business.*','ib') then 'Control-First/Biz'
-- when site_id_dcm='1853562' and (p1.placement  like '.*Control.*')  then 'Control'
-- when site_id_dcm='1853562' and (p1.placement  like '.*Test.*')  then 'Test'
else 'Other' end as cat,
        'activity' as tbl,
--         p1.placement,
        row_number() over (partition by user_id order by user_id) as u1,

                                sum(total_conversions) as trans
--                                 row_number() over ()                        as cvr_nbr
--                              count(*) as trans
                from
                                diap01.mec_us_united_20056.dfa2_activity    as t1
                -- these JOINs increase runtime significantly, but we need placement names to filter Sojern's tactics
                left join
                                diap01.mec_us_united_20056.dfa2_placements  as p1
                                on t1.placement_id = p1.placement_id
                where
                                t1.activity_id = 978826 and
                                t1.campaign_id = 20606595 and
                                user_id != '0' and
                                (t1.site_id_dcm = 1239319 and regexp_like(placement,'.*_RON_.*','ib')) and

                                cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2018-01-14' and '2018-01-27' and
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
                                when regexp_like(p1.placement, '.*BusinessClassTicket.*' ,'ib')  then 'Biz'
                                when regexp_like(p1.placement, '.*EconomyNoBidStrategy.*' ,'ib') then 'Econ'
                                when regexp_like(p1.placement, '.*FirstClass.*' ,'ib')           then 'First'
                                when regexp_like(p1.placement, '.*CatchAll1.*' ,'ib')            then 'CatchAll1'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy1.*','ib') and regexp_like(p1.placement, '.*General.*','ib') then 'Test-Bid1-Member'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy1.*','ib') and regexp_like(p1.placement, '.*NonMember.*','ib') then 'Test-Bid1-NonMember'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy2.*','ib') and regexp_like(p1.placement, '.*General.*','ib') then 'Test-Bid2-Member'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy2.*','ib') and regexp_like(p1.placement, '.*NonMember.*','ib') then 'Test-Bid2-NonMember'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy3.*','ib') and regexp_like(p1.placement, '.*General.*','ib') then 'Test-Bid3-Member'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*BidStrategy3.*','ib') and regexp_like(p1.placement, '.*NonMember.*','ib') then 'Test-Bid3-NonMember'
                                when regexp_like(p1.placement, '.*Control.*','ib')  and regexp_like(p1.placement, '.*BidStrategy1.*','ib') then 'Control-Bid1'
                                when regexp_like(p1.placement, '.*Control.*','ib')  and regexp_like(p1.placement, '.*BidStrategy2.*','ib') then 'Control-Bid2'
                                when regexp_like(p1.placement, '.*Control.*','ib')  and regexp_like(p1.placement, '.*BidStrategy3.*','ib') then 'Control-Bid3'
                                when regexp_like(p1.placement, '.*Test.*'   ,'ib')  and regexp_like(p1.placement, '.*First.*','ib') and regexp_like(p1.placement, '.*Business.*','ib') then 'Test-First/Biz'
                                when regexp_like(p1.placement, '.*Control.*','ib')  and regexp_like(p1.placement, '.*First.*','ib') and regexp_like(p1.placement, '.*Business.*','ib') then 'Control-First/Biz'
                                -- when site_id_dcm='1853562' and (p1.placement  like '.*Control.*')  then 'Control'
                                -- when site_id_dcm='1853562' and (p1.placement  like '.*Test.*')  then 'Test'
                                else 'Other' end as cat,
        'impression' as tbl,
--         p1.placement,
         count(1) over (partition by user_id) as u1,
                                0 as trans
                from
                                diap01.mec_us_united_20056.dfa2_impression      as t2
                left join
                                diap01.mec_us_united_20056.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                user_id != '0' and
                                (t2.site_id_dcm = 1239319 and regexp_like(placement,'.*_RON_.*','ib')) and
                                t2.campaign_id = 20606595 and
                                cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-01-14' and '2018-01-27' and
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