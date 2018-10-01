-- 20160831 Search window queries
-- 1.) query to identify evaluate time-to-search, count users, place them in bins (buckets of ten days)
-- 2.) query to count all users who purchased a ticket

-- // Finds users who purchased a ticket and THEN performed a subsequent route search; multiple transactions by the same user are counted as long as each is followed by a route search
select
--  month(cast(tix_time as date)),
--  grouped,
--  count(sch_nbr) as your_mom,
  cvr_window_day,
  class,
  day_bins,
  count(sch_nbr) as user_count

from (
       select
         c.user_id,
--    order_id,
         c.tix_time,
         c.sch_nbr,
         c.sch_time,
--  cvr_window_sec as cvr_window_sec,
         c.cvr_window_day as cvr_window_day,
         c.cvr_window_mth as cvr_window_mth,
         case
         when datediff('day',c.tix_time,c.sch_time) between 0 and 9 then '0-9'
         when datediff('day',c.tix_time,c.sch_time) between 10 and 19 then '10-19'
         when datediff('day',c.tix_time,c.sch_time) between 20 and 29 then '20-29'
         when datediff('day',c.tix_time,c.sch_time) between 30 and 39 then '30-39'
         when datediff('day',c.tix_time,c.sch_time) between 40 and 49 then '40-49'
         when datediff('day',c.tix_time,c.sch_time) between 50 and 59 then '50-59'
         when datediff('day',c.tix_time,c.sch_time) between 60 and 69 then '60-69'
         when datediff('day',c.tix_time,c.sch_time) between 70 and 79 then '70-79'
         when datediff('day',c.tix_time,c.sch_time) between 80 and 89 then '80-89'
         when datediff('day',c.tix_time,c.sch_time) between 90 and 99 then '90-99'
         when datediff('day',c.tix_time,c.sch_time) between 100 and 109 then '100-109'
         when datediff('day',c.tix_time,c.sch_time) between 110 and 119 then '110-119'
         when datediff('day',c.tix_time,c.sch_time) between 120 and 129 then '120-129'
         when datediff('day',c.tix_time,c.sch_time) between 130 and 139 then '130-139'
         when datediff('day',c.tix_time,c.sch_time) between 140 and 149 then '140-149'
         when datediff('day',c.tix_time,c.sch_time) between 150 and 159 then '150-159'
         when datediff('day',c.tix_time,c.sch_time) between 160 and 169 then '160-169'
         when datediff('day',c.tix_time,c.sch_time) between 170 and 179 then '170-179'
         when datediff('day',c.tix_time,c.sch_time) between 180 and 189 then '180-189'
         when datediff('day',c.tix_time,c.sch_time) between 190 and 199 then '190-199'
         when datediff('day',c.tix_time,c.sch_time) between 200 and 209 then '200-209'
         when datediff('day',c.tix_time,c.sch_time) between 210 and 219 then '210-219'
         when datediff('day',c.tix_time,c.sch_time) between 220 and 229 then '220-229'
         else 'other'
         end              as day_bins,

         traveldate_1,
         case
         when datediff('day',c.tix_time,c.traveldate_1) > 7 then 'personal'   -- when days between purchase and first travel date is > 7, then we call it "personal;" otherwise, assume it's "business"
         else 'business'
         end              as class,
         c.r1
       from
         (select
            a.user_id,
--        a.order_id,
            a.tix_time,
            b.sch_nbr,
            b.sch_time,
            a.traveldate_1,
            -- difference, in days, between ticket purchase and first travel dates
            datediff('day',a.tix_time,a.traveldate_1)                                                           as pch_window_day,
            -- difference, in seconds, between ticket purchase and route search dates
            datediff('ss',a.tix_time,b.sch_time)                                                                as cvr_window_sec,
            -- difference, in days, between ticket purchase and route search dates
            datediff('day',a.tix_time,b.sch_time)                                                               as cvr_window_day,
            -- difference, in months, between ticket purchase and route search dates
            datediff('month',a.tix_time,b.sch_time)                                                             as cvr_window_mth,
            -- Create field that numbers each row, starting at one; group it by user_id and ticket purchase time; sort it by the difference, in seconds, between ticket purchase and route search times. This means that each unique user will have their purchases (if there are more than one) numbered, "1" being the most recent
            row_number() over (partition by a.user_id,a.tix_time order by datediff('ss',a.tix_time,b.sch_time)) as r1
          from
            (select   -- query activity table to get ticket purchases
               user_id,
               activity_time                                                        as tix_time,
               cast(subString(other_data,(INSTR(other_data,'u9=') + 3),10) as date) as traveldate_1   -- first travel date
             from
               mec.UnitedUS.dfa_activity
             where
               revenue != 0 and
               quantity != 0 and
               activity_time ::date between '2016-01-01' and '2016-07-31'
                               -- advertiser_id != 0 and    -- exclude records that are not media-attributed (United)
                               and activity_type = 'ticke498' and activity_sub_type = 'unite820'    -- ticket confirmation
                               and user_id != '0'   -- exclude cookies with no ID
            ) as a,
            (select   -- query activity table to get route searches
               user_id,
               activity_time                                                   as sch_time,
               row_number() over (partition by user_id order by activity_time) as sch_nbr   -- arbitrary row numbers; grouped by user_id; sorted by time; gives us something to count later
             from
               mec.UnitedUS.dfa_activity
             where
               activity_type = 'unite280' and activity_sub_type = 'unite229' -- search results
               and user_id != '0'   -- exclude cookies with no ID
            ) as b
          where
            a.user_id = b.user_id and
            sch_time > tix_time
         ) as c
       where
         r1 = 1 and
         c.traveldate_1 is not null   -- exclude records without machine-readable first travel date
       group by
         c.user_id,
--    order_id,
         c.tix_time,
         c.sch_nbr,
         c.sch_time,
--  cvr_window_sec,
         c.cvr_window_day,
         c.cvr_window_mth,
         c.traveldate_1,
--           c.grouped,
         c.r1

     ) as d
group by
--  month(cast(tix_time as date))
  class,
  day_bins,
  cvr_window_day
--  grouped
;

-- // =========================================================================================================================================================
-- // Finds all users who purchased a ticket; multiple transactions by the same user are counted
select
  month(cast(t2.act_date as date)) as act_date,
  count(user_id)                   as users,
  sum(t2.trans)                    as trans,
  sum(t2.quantity)                 as quantity
from (
       select
         t1.user_id       as user_id,
         t1.activity_time as act_date,
         count(*)         as trans,
         sum(t1.quantity) as quantity
       from
         mec.UnitedUS.dfa_activity as t1
       where
         revenue != 0 and
         quantity != 0 and
         activity_time ::date between '2016-01-01' and '2016-08-31'
         and advertiser_id != 0 -- comment out to count users transactions unattributed to media
         and user_id != '0' -- comment out to count all users, even those with a cookie ID of 0
         and (activity_type = 'ticke498' and activity_sub_type = 'unite820')
       group by
         t1.user_id,
         t1.activity_time
     ) as t2
group by month(cast(t2.act_date as date))