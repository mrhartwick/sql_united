select
                cast(userid as varchar(128)) as c_id,
                conversiondate as cvr_time,
                sum(1) as its_con

from
               wmprodfeeds.ikea.sizmek_conversion_events       as t2
where             userid != '0'
--                 and eventtypeid = 1
-- campaignids in Qualia impression
and campaignid in (913795, 148109, 813465, 876944, 913509, 850408, 908875, 912805, 914697, 157710, 139540, 858670, 835032, 939667, 913792, 908018, 862917, 913772, 813500, 162125, 822379, 823989, 955274)
             -- ITS Conversions
and (
    (conversiontagid = 596537 and string9 like 'high%') or
    (conversiontagid = 1244766 and string8 like 'high%') or
    (conversiontagid in (593715, 1061036)))
and isconversion = TRUE
--              States with IKEAs in them
and stateid in (3, 5, 7, 10, 11, 14, 15, 17, 21, 23, 24, 26, 29, 31, 33, 34, 36, 38, 39, 43, 44, 45, 47, 48, 50)
and             conversiondate::date between '2017-09-01' and '2019-03-31'
group by cast(userid as varchar(128)), conversiondate