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
--              case
--              when datediff('month',c.tix_time,c.sch_time) = 0 and datediff('day',c.tix_time,c.sch_time) >= 20 then '20-30 days'
--              when datediff('month',c.tix_time,c.sch_time) > 6 and datediff('month',c.tix_time,c.sch_time) <= 12 then '6-12 months'
--              when datediff('month',c.tix_time,c.sch_time) = 1 then '1 month'
--              when datediff('month',c.tix_time,c.sch_time) = 2 then '2 months'
--              when datediff('month',c.tix_time,c.sch_time) = 3 then '3 months'
--              when datediff('month',c.tix_time,c.sch_time) = 4 then '4 months'
--              when datediff('month',c.tix_time,c.sch_time) = 5 then '5 months'
--              when datediff('month',c.tix_time,c.sch_time) = 6 then '6 months'
--              when datediff('month',c.tix_time,c.sch_time) > 12 then 'year +'
--              when datediff('day',c.tix_time,c.sch_time) = 0 then 'same day'
--              when datediff('day',c.tix_time,c.sch_time) = 1 then '1 day'
--              when datediff('day',c.tix_time,c.sch_time) = 2 then '2 days'
--              when datediff('day',c.tix_time,c.sch_time) = 3 then '3 days'
--              when datediff('day',c.tix_time,c.sch_time) = 4 then '4 days'
--              when datediff('day',c.tix_time,c.sch_time) = 5 then '5 days'
--              when datediff('day',c.tix_time,c.sch_time) = 6 then '6 days'
--              when datediff('day',c.tix_time,c.sch_time) = 7 then '7 days'
--              when datediff('day',c.tix_time,c.sch_time) = 8 then '8 days'
--              when datediff('day',c.tix_time,c.sch_time) = 9 then '9 days'
--              when datediff('day',c.tix_time,c.sch_time) = 10 then '10 days'
--              when datediff('day',c.tix_time,c.sch_time) = 11 then '11 days'
--              when datediff('day',c.tix_time,c.sch_time) = 12 then '12 days'
--              when datediff('day',c.tix_time,c.sch_time) = 13 then '13 days'
--              when datediff('day',c.tix_time,c.sch_time) = 14 then '14 days'
--              when datediff('day',c.tix_time,c.sch_time) = 15 then '15 days'
--              when datediff('day',c.tix_time,c.sch_time) = 16 then '16 days'
--              when datediff('day',c.tix_time,c.sch_time) = 17 then '17 days'
--              when datediff('day',c.tix_time,c.sch_time) = 18 then '18 days'
--              when datediff('day',c.tix_time,c.sch_time) = 19 then '19 days'
--              end              as grouped,

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
             when datediff('day',c.tix_time,c.traveldate_1) > 7 then 'personal'
                 else 'business'
             end              as class,
             c.r1
         from
             ( select
                   a.user_id,
--        a.order_id,
                   a.tix_time,
                   b.sch_nbr,
                   b.sch_time,
                   a.traveldate_1,
                   datediff('day',a.tix_time,a.traveldate_1)                                                           as pch_window_day,
                   datediff('ss',a.tix_time,b.sch_time)                                                                as cvr_window_sec,
                   datediff('day',a.tix_time,b.sch_time)                                                               as cvr_window_day,
                   datediff('month',a.tix_time,b.sch_time)                                                             as cvr_window_mth,
                   row_number() over (partition by a.user_id,a.tix_time order by datediff('ss',a.tix_time,b.sch_time)) as r1
               from
                   ( select
                         user_id,
                         activity_time as tix_time,
                       cast(subString(other_data,( INSTR(other_data,'u9=') + 3 ),10) as date) as traveldate_1
                     --                          row_number() over (partition by user_id order by activity_time) as sch_nbr
                     from
                         mec.UnitedUS.dfa_activity
                     where
--                       user_id = 'AMsySZY-c4KkfelaiPFa60N5oZzt' and
--                       user_id in ('AMsySZZiHBCWQjp2vJhEUQR_Qx1T', 'AMsySZYz8UCa9bCS-xNdJ-LIDUoW', 'AMsySZYyoDt0DQfbxjT-GWqT1il1', 'AMsySZa2LMuKJsjy057wcfVedydA', 'AMsySZb9aHI2WgGzF-klMrb_MZqc', 'AMsySZZvvr67w64JfUhNdBZziJgn', 'AMsySZbcl5slolFFeIQUPzv46ce_', 'AMsySZZ2i-DaWRbfJEKuw5M6a_Wf', 'AMsySZb1UlY0WfCfFQ1lYYds_z5b', 'AMsySZad4CSsihVR38Zcp6fVsXJK', 'AMsySZZsc8OKckDAOy4HOhMexY0v', 'AMsySZZzt4E30_XTNzmg799npCOv', 'AMsySZYniUHAalFu_VaVJD0suhgC', 'AMsySZZrKJy5nVRS5-UPDuKbRQ7A', 'AMsySZYsqIuzFAB18tg4XV800I4B', 'AMsySZY-NpMufD_EFqWINCXoF9DW', 'AMsySZbmVBLRITnL7By9j2JstNHl', 'AMsySZZyXcXtVchKfNqAy_E9QuX1', 'AMsySZY_2OTRAJ-gva2mWabz9MU2', 'AMsySZZG3vUOTbMUIgACG0ZG68Fu', 'AMsySZYqb6sWykbhWES0suQjfgpu', 'AMsySZa907i-zbWA5TvzYO6IVwyh', 'AMsySZb3W1r1ltG-5tMXFPNoWbRY', 'AMsySZaeuMEyxMcy5SA28hcFXKZU', 'AMsySZaFxmPs-2dl2qUbNbZdcAni', 'AMsySZbd8jwvP1M6b2lVK0ZV5iyF', 'AMsySZbwXxw8Z3VYfK23eXYIU36G', 'AMsySZb6cGZftOH1Btak55jxKTl1', 'AMsySZYrmI4m3M0b5o-3-wv964I-', 'AMsySZYT03IOMCLlfITOEZJKQFvw', 'AMsySZbWJK-1Xl871zBiw2HxPE3U', 'AMsySZbnHWtLt-0AzaEMXOjHD3lM', 'AMsySZYV15VZj0L_VrxrMmGHKO_v', 'AMsySZaEJ65Xt7IRECfbfXkXmayH', 'AMsySZaObsTUgl1gTMp1ISh_ENU-', 'AMsySZYPT99tNy4nDKNhwevKphGu', 'AMsySZabGtYKcRG1SBfOJaY8xCU1', 'AMsySZZygtvbSHU1Vo2vUx5dudLt', 'AMsySZaI7A8ZA5aGk3zH3RhnqtF7', 'AMsySZYs5EppRVDTpPbOKNb3pqeD', 'AMsySZZK0AbYWw4vL5NvlwWBoRJd', 'AMsySZaScKTo7foEIBN8bz547Vlo', 'AMsySZa6CXxgsAWOHDKHYXECQWSu', 'AMsySZatIiO0W0lOmgeJu87eS9r7', 'AMsySZbi213XnGn-gAQeOinFySjK', 'AMsySZbCW1I4SiSppze_ITTjwBce', 'AMsySZZn2jLkA1K83ZkMcsV2vHjQ', 'AMsySZZDMtC3bccK0TpEzcSDrkkV', 'AMsySZbBvkB8Jds6BeLVekhjgsYY', 'AMsySZZoTATMKjK7yso2a_3-hilc', 'AMsySZZrKJy5nVRS5-UPDuKbRQ7A', 'AMsySZateCZn63PhNkSr0ID6-f2i', 'AMsySZatUfVZXM72lDSGcT2VgIbd', 'AMsySZZnosSdjhnjnDjnNE_H9096', 'AMsySZYXjUBYIzGlMt1b_e3mSX73', 'AMsySZbb4BfM84WyXCK0lwZEdJms', 'AMsySZa1sTZWcP2SBy6SH300rVq8', 'AMsySZbesQT9QVAIjZE1J_UxrVIB', 'AMsySZbiQjmJ3KXvc15qLAkePd63', 'AMsySZa3v2TIb55HLb4dR9_L692h', 'AMsySZaWFSrZZdYSGYXS5qGFstFh', 'AMsySZbrF_HoGXnR_X6BqoeZB9Dt', 'AMsySZbu_xafp3JNcFHLwOuqf2Ju', 'AMsySZYPiY7hLqMOusoZ4pJGXggL', 'AMsySZa-hovKoeeYCGBhjuSPSp6s', 'AMsySZYRUtOQnUHmqFcyhq9zqtrX', 'AMsySZYJ6kL1tMEZqvbadELhYHJU', 'AMsySZZ5TNf8Z4foJHoNDEAS30zk', 'AMsySZbXhUOJM5U6uTea4CyQ4LtQ', 'AMsySZbJC7OOA9yGp-d98CbxQhjn', 'AMsySZbfJjqHYXnBI7DUKeThVIeY', 'AMsySZb2V6oAAmK2BlGpGRDesdIP', 'AMsySZad3Ky6nwdL_Pr3j1a5Srsh', 'AMsySZbSPG-FAE5TE8mxiuwbQWpb', 'AMsySZZxnofAj5rae9SQsCJr-Xbf', 'AMsySZaMnM1En1i4C0dp9TDXY3kf', 'AMsySZZtW8sMe1rdO5h8-4n2ciAb', 'AMsySZZNVjMb5UKm1aA7iQW_M72L', 'AMsySZaSD93HgAlimNNqoRJdDOZ1', 'AMsySZbzuUni2-67JyGVJAoCGLyw', 'AMsySZZVyi_GKH-zysD9rUFAVv2h', 'AMsySZat40Rzqx9WA9V_PW0vh7gZ', 'AMsySZbe4nOiaQ1CnuOsHBCpKUm3', 'AMsySZbdb9GXRMK-WXa-DXtENSHg', 'AMsySZa2GnheI8s0jKsKq49-ABRb', 'AMsySZYr9y5bT_rs3VowFYGqmx0u', 'AMsySZYHeHzAwzFMWJidG4vbm9al', 'AMsySZauS49NcKx3c5-kuNR0LOoz', 'AMsySZbtOZnVBlC7n_71gaFGjD7X', 'AMsySZbrEfvE8b_yoMkVSbFXeKD_', 'AMsySZaugxulTMsApwFfBQko4-i0', 'AMsySZY8VJc7ipjmwerZTpaQ07ne', 'AMsySZb1ip8OyJUQl7_3SkZxi7GV', 'AMsySZbdVmvv0BuLFHG4RaczZAqy', 'AMsySZaPN43Any9D36iUHScLorWa', 'AMsySZZJQFXcof-z9rqMOQgPzJCA', 'AMsySZZleqNZI5XGU2Gi2Gh3YwN-', 'AMsySZbtMI4gJvXAsoX2EAF4n-iI', 'AMsySZZO49_d0WXkJQq6Nlu9uZlZ', 'AMsySZbOFLl4VpycKp-kJ9jlUZmo') and
                         revenue != 0 and
                         quantity != 0 and
                         activity_time ::date between '2016-01-01' and '2016-07-31' and
                                         --             activity_time:: = '7/1/2016'  AND
                                         advertiser_id != 0 and
                                         --                             order_id not in ( 9830589,9860136,9501268 ) -- unidentified campaigns
                                         --                                          order_id in ( 8441345,9349302,9917294,8063775,9961760,8161352,6146233 )  -- Search
                                         order_id in
                                         ( 9999841,8945683,8958859,10121649,9109393,8955169,8493481,8581015,8755908,8837963,8504608,9205780,9639387,9408733,9407915,10094548,9973506,9994694,9923634,9630239,9548151,9739006,9304728,9836967,8441345,9349302,9917294,8063775,9961760,8161352,6146233 ) -- Display and Search
--                                      order_id in (6146233, 9304728, 9407915, 9408733, 9548151, 9630239, 9639387, 9739006, 9923634, 9973506, 9994694, 9999841, 10094548, 10121649, 8063775, 9349302, 8161352, 9917294, 8958859, 9836967, 9961760) -- Display and Search 2016
                                         --          group by user_id,activity_time
                                         and ( activity_type = 'ticke498' and activity_sub_type = 'unite820' )

                   ) as a,
                   ( select
                         user_id,
                         activity_time                                                   as sch_time,
                         row_number() over (partition by user_id order by activity_time) as sch_nbr
                     from
                         mec.UnitedUS.dfa_activity
                     where
--                       user_id = 'AMsySZY-c4KkfelaiPFa60N5oZzt' and
--                       user_id in ('AMsySZZiHBCWQjp2vJhEUQR_Qx1T', 'AMsySZYz8UCa9bCS-xNdJ-LIDUoW', 'AMsySZYyoDt0DQfbxjT-GWqT1il1', 'AMsySZa2LMuKJsjy057wcfVedydA', 'AMsySZb9aHI2WgGzF-klMrb_MZqc', 'AMsySZZvvr67w64JfUhNdBZziJgn', 'AMsySZbcl5slolFFeIQUPzv46ce_', 'AMsySZZ2i-DaWRbfJEKuw5M6a_Wf', 'AMsySZb1UlY0WfCfFQ1lYYds_z5b', 'AMsySZad4CSsihVR38Zcp6fVsXJK', 'AMsySZZsc8OKckDAOy4HOhMexY0v', 'AMsySZZzt4E30_XTNzmg799npCOv', 'AMsySZYniUHAalFu_VaVJD0suhgC', 'AMsySZZrKJy5nVRS5-UPDuKbRQ7A', 'AMsySZYsqIuzFAB18tg4XV800I4B', 'AMsySZY-NpMufD_EFqWINCXoF9DW', 'AMsySZbmVBLRITnL7By9j2JstNHl', 'AMsySZZyXcXtVchKfNqAy_E9QuX1', 'AMsySZY_2OTRAJ-gva2mWabz9MU2', 'AMsySZZG3vUOTbMUIgACG0ZG68Fu', 'AMsySZYqb6sWykbhWES0suQjfgpu', 'AMsySZa907i-zbWA5TvzYO6IVwyh', 'AMsySZb3W1r1ltG-5tMXFPNoWbRY', 'AMsySZaeuMEyxMcy5SA28hcFXKZU', 'AMsySZaFxmPs-2dl2qUbNbZdcAni', 'AMsySZbd8jwvP1M6b2lVK0ZV5iyF', 'AMsySZbwXxw8Z3VYfK23eXYIU36G', 'AMsySZb6cGZftOH1Btak55jxKTl1', 'AMsySZYrmI4m3M0b5o-3-wv964I-', 'AMsySZYT03IOMCLlfITOEZJKQFvw', 'AMsySZbWJK-1Xl871zBiw2HxPE3U', 'AMsySZbnHWtLt-0AzaEMXOjHD3lM', 'AMsySZYV15VZj0L_VrxrMmGHKO_v', 'AMsySZaEJ65Xt7IRECfbfXkXmayH', 'AMsySZaObsTUgl1gTMp1ISh_ENU-', 'AMsySZYPT99tNy4nDKNhwevKphGu', 'AMsySZabGtYKcRG1SBfOJaY8xCU1', 'AMsySZZygtvbSHU1Vo2vUx5dudLt', 'AMsySZaI7A8ZA5aGk3zH3RhnqtF7', 'AMsySZYs5EppRVDTpPbOKNb3pqeD', 'AMsySZZK0AbYWw4vL5NvlwWBoRJd', 'AMsySZaScKTo7foEIBN8bz547Vlo', 'AMsySZa6CXxgsAWOHDKHYXECQWSu', 'AMsySZatIiO0W0lOmgeJu87eS9r7', 'AMsySZbi213XnGn-gAQeOinFySjK', 'AMsySZbCW1I4SiSppze_ITTjwBce', 'AMsySZZn2jLkA1K83ZkMcsV2vHjQ', 'AMsySZZDMtC3bccK0TpEzcSDrkkV', 'AMsySZbBvkB8Jds6BeLVekhjgsYY', 'AMsySZZoTATMKjK7yso2a_3-hilc', 'AMsySZZrKJy5nVRS5-UPDuKbRQ7A', 'AMsySZateCZn63PhNkSr0ID6-f2i', 'AMsySZatUfVZXM72lDSGcT2VgIbd', 'AMsySZZnosSdjhnjnDjnNE_H9096', 'AMsySZYXjUBYIzGlMt1b_e3mSX73', 'AMsySZbb4BfM84WyXCK0lwZEdJms', 'AMsySZa1sTZWcP2SBy6SH300rVq8', 'AMsySZbesQT9QVAIjZE1J_UxrVIB', 'AMsySZbiQjmJ3KXvc15qLAkePd63', 'AMsySZa3v2TIb55HLb4dR9_L692h', 'AMsySZaWFSrZZdYSGYXS5qGFstFh', 'AMsySZbrF_HoGXnR_X6BqoeZB9Dt', 'AMsySZbu_xafp3JNcFHLwOuqf2Ju', 'AMsySZYPiY7hLqMOusoZ4pJGXggL', 'AMsySZa-hovKoeeYCGBhjuSPSp6s', 'AMsySZYRUtOQnUHmqFcyhq9zqtrX', 'AMsySZYJ6kL1tMEZqvbadELhYHJU', 'AMsySZZ5TNf8Z4foJHoNDEAS30zk', 'AMsySZbXhUOJM5U6uTea4CyQ4LtQ', 'AMsySZbJC7OOA9yGp-d98CbxQhjn', 'AMsySZbfJjqHYXnBI7DUKeThVIeY', 'AMsySZb2V6oAAmK2BlGpGRDesdIP', 'AMsySZad3Ky6nwdL_Pr3j1a5Srsh', 'AMsySZbSPG-FAE5TE8mxiuwbQWpb', 'AMsySZZxnofAj5rae9SQsCJr-Xbf', 'AMsySZaMnM1En1i4C0dp9TDXY3kf', 'AMsySZZtW8sMe1rdO5h8-4n2ciAb', 'AMsySZZNVjMb5UKm1aA7iQW_M72L', 'AMsySZaSD93HgAlimNNqoRJdDOZ1', 'AMsySZbzuUni2-67JyGVJAoCGLyw', 'AMsySZZVyi_GKH-zysD9rUFAVv2h', 'AMsySZat40Rzqx9WA9V_PW0vh7gZ', 'AMsySZbe4nOiaQ1CnuOsHBCpKUm3', 'AMsySZbdb9GXRMK-WXa-DXtENSHg', 'AMsySZa2GnheI8s0jKsKq49-ABRb', 'AMsySZYr9y5bT_rs3VowFYGqmx0u', 'AMsySZYHeHzAwzFMWJidG4vbm9al', 'AMsySZauS49NcKx3c5-kuNR0LOoz', 'AMsySZbtOZnVBlC7n_71gaFGjD7X', 'AMsySZbrEfvE8b_yoMkVSbFXeKD_', 'AMsySZaugxulTMsApwFfBQko4-i0', 'AMsySZY8VJc7ipjmwerZTpaQ07ne', 'AMsySZb1ip8OyJUQl7_3SkZxi7GV', 'AMsySZbdVmvv0BuLFHG4RaczZAqy', 'AMsySZaPN43Any9D36iUHScLorWa', 'AMsySZZJQFXcof-z9rqMOQgPzJCA', 'AMsySZZleqNZI5XGU2Gi2Gh3YwN-', 'AMsySZbtMI4gJvXAsoX2EAF4n-iI', 'AMsySZZO49_d0WXkJQq6Nlu9uZlZ', 'AMsySZbOFLl4VpycKp-kJ9jlUZmo') and
--                                                   activity_time ::date between '2016-06-01' and '2016-07-31' and
                         --                             order_id not in ( 9830589,9860136,9501268 ) -- unidentified campaigns
                         --                                               order_id in ( 8441345,9349302,9917294,8063775,9961760,8161352,6146233 )  -- Search
                         order_id in
                                         ( 9999841,8945683,8958859,10121649,9109393,8955169,8493481,8581015,8755908,8837963,8504608,9205780,9639387,9408733,9407915,10094548,9973506,9994694,9923634,9630239,9548151,9739006,9304728,9836967,8441345,9349302,9917294,8063775,9961760,8161352,6146233 ) -- Display and Search
--                                      order_id in (6146233, 9304728, 9407915, 9408733, 9548151, 9630239, 9639387, 9739006, 9923634, 9973506, 9994694, 9999841, 10094548, 10121649, 8063775, 9349302, 8161352, 9917294, 8958859, 9836967, 9961760) -- Display and Search 2016
                         and ( activity_type = 'unite280' and activity_sub_type = 'unite229' )
                       and user_id != '0'
                   ) as b
               where
                   a.user_id = b.user_id and
                   sch_time > tix_time
--                 and
--                 sch_nbr = 1

             ) as c
         where
             r1 = 1 and
                 c.traveldate_1 is not null
         group by
             c.user_id,
--    order_id,
             c.tix_time,
             c.sch_nbr,
             c.sch_time,
--  cvr_window_sec as cvr_window_sec,
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
