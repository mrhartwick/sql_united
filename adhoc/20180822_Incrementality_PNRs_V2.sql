select a.campaign,
            md_event_date_loc as conversion_date,
            a.user_id,
            a.pnr,
            md_interaction_date_loc as media_date,
            count(distinct cvr_nbr) as conversions,
            sum(tix) as tix
        --                 imp_nbr
     from (select case when regexp_instr(p1.placement, '.*Incrementality-Test-Exposed.*') > 0 then 'Exposed'
                       when regexp_instr(p1.placement, '.*Incrementality-Test-Non-Exposed.*') > 0 then 'Non-Exposed'
                       else 'XXX' end as campaign,
--                                 event_time,
                  md_event_date_loc,
                  user_id,
                  udf_base64decode(left(right(other_data, len(other_data) - position('u18=' in other_data) - 3),
                                        position(';' in
                                                 right(other_data, len(other_data) - position('u18=' in other_data) - 3)) -
                                            1)) as pnr,
--                                 interaction_time,
                  md_interaction_date_loc,
                  total_conversions as tix,
                  row_number() over () as cvr_nbr
           from wmprodfeeds.united.dfa2_activity as t1
                    left join wmprodfeeds.united.dfa2_placements as p1 on t1.placement_id = p1.placement_id
           where t1.activity_id = 978826 and t1.campaign_id = 20606595 and user_id != '0' and md_interaction_date_loc between '2018-08-16' and '2018-08-20' and advertiser_id <> 0 and regexp_instr(p1.placement, '.*Incrementality-Test.*') and
                       total_conversions > 0

          ) as a

     group by a.campaign,
              md_event_date_loc,
              a.user_id,
              a.pnr,
              md_interaction_date_loc;
-- ================================================================================================================
select campaign,
       md_event_date_loc as impression_date,
       user_id,
       count(distinct imp_nbr) as impressions

from (select case when regexp_instr(p1.placement, '.*Incrementality-Test-Exposed.*') > 0 then 'Exposed'
                  when regexp_instr(p1.placement, '.*Incrementality-Test-Non-Exposed.*') > 0 then 'Non-Exposed'
                  else 'XXX' end as campaign,
             user_id,
             md_event_date_loc,
             row_number() over () as imp_nbr
      from wmprodfeeds.united.dfa2_impression as t1
               left join wmprodfeeds.united.dfa2_placements as p1 on t1.placement_id = p1.placement_id
      where t1.campaign_id = 20606595 and user_id != '0' and md_event_date_loc between '2018-08-16' and '2018-08-20' and advertiser_id <> 0 and regexp_instr(p1.placement, '.*Incrementality-Test.*') > 0) as t2

group by campaign,
         md_event_date_loc,
         user_id