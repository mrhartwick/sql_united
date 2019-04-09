
select distinct
campaignname, campaignid
from wmprodfeeds.ikea.sizmek_display_campaigns
where campaignname like '%FY18%'








	select
	       s1.sitename,
	       t1.siteid,
			count(distinct userid) as users,
	       count(*) as imps

	from wmprodfeeds.ikea.sizmek_standard_events t1

	left join wmprodfeeds.ikea.sizmek_sites s1
	on t1.siteid = s1.siteid

	where eventtypeid = 1 and eventdatedefaulttimezone::date between '2018-10-18' and '2018-12-25'
	      and (advertiserid != 0)
	and (campaignid =913795 or campaignid =913509)

	group by
           s1.sitename,
	       t1.siteid

-- ===================================================================================================================
-- FY19 Summary

select
-- 	       eventdatedefaulttimezone::date,
           t1.sitename,
	       t1.siteid,
-- 	       userid,
--            c1.campaignname,
-- 	       t1.campaignid,
           count(distinct userid) as users,
		   sum(imps) as imps


from
(
	select
	       eventdatedefaulttimezone,
	       userid,
	       s1.sitename,
	       t0.siteid,
	       campaignid,
	       1 as imps

	from wmprodfeeds.ikea.sizmek_standard_events t0

	left join wmprodfeeds.ikea.sizmek_sites s1
	on t0.siteid = s1.siteid

	where eventtypeid = 1 and eventdatedefaulttimezone::date between '2018-09-01' and '2019-02-19'
	      and (advertiserid != 0)
-- 	and campaignid in (908875, 912805, 913509, 913772, 913792, 913795, 914697, 939667, 950456, 955274)
    and (t0.campaignid =913795 or t0.campaignid =913509)
    and t0.siteid is not null
-- 	group by
-- 	         eventdatedefaulttimezone,
-- 	         userid
-- -- 	         campaignid
) t1

-- left join wmprodfeeds.ikea.sizmek_display_campaigns c1
-- on t1.campaignid = c1.campaignid

group by
-- 	       eventdatedefaulttimezone::date,
                    t1.sitename,
	       t1.siteid
-- -- 	       userid,
--            c1.campaignname,
-- 	       t1.campaignid
-- ===================================================================================================================
-- FY18 Summary

select
	       eventdatedefaulttimezone::date,
           t1.sitename,
	       t1.siteid,
-- 	       userid,
--            c1.campaignname,
-- 	       t1.campaignid,
           count(distinct userid) as users,
		   sum(imps) as imps


from
(
	select
	       eventdatedefaulttimezone,
	       userid,
	       s1.sitename,
	       t0.siteid,
	       campaignid,
	       1 as imps

	from wmprodfeeds.ikea.sizmek_standard_events t0

	left join wmprodfeeds.ikea.sizmek_sites s1
	on t0.siteid = s1.siteid

	where eventtypeid = 1 and eventdatedefaulttimezone::date between '2017-09-01' and '2018-07-31'
	      and (advertiserid != 0)
-- 	and campaignid in (908875, 912805, 913509, 913772, 913792, 913795, 914697, 939667, 950456, 955274)
    and t0.campaignid = 813500
--     and t0.siteid is not null
-- 	group by
-- 	         eventdatedefaulttimezone,
-- 	         userid
-- -- 	         campaignid
) t1

-- left join wmprodfeeds.ikea.sizmek_display_campaigns c1
-- on t1.campaignid = c1.campaignid

group by
	       eventdatedefaulttimezone::date,
                    t1.sitename,
	       t1.siteid
-- -- 	       userid,
--            c1.campaignname,
-- 	       t1.campaignid

-- ===================================================================================================================

select * from wmprodfeeds.ikea.sizmek_standard_events
where eventdatedefaulttimezone::date = '2017-11-11'
-- and campaignid = 858670
limit 100


select * from wmprodfeeds.ikea.sizmek_conversion_events
where conversiondatedefaulttimezone::date = '2017-11-11'
and campaignid = 813500
limit 100

-- ===================================================================================================================
select
	       eventdatedefaulttimezone::date,
-- 	       userid,
--            t1.sitename,
-- 	       t1.siteid,
           t1.campaignname,
	       t1.campaignid,
           count(distinct userid) as users,
		   sum(imps) as imps


from
(
	select
	       eventdatedefaulttimezone,
	       userid,
           s1.sitename,
	       t1.siteid,
	       c1.campaignname,
	       t1.campaignid,
	       1 as imps

	from wmprodfeeds.ikea.sizmek_standard_events t1

	left join wmprodfeeds.ikea.sizmek_display_campaigns c1
on t1.campaignid = c1.campaignid

		left join wmprodfeeds.ikea.sizmek_sites s1
	on t1.siteid = s1.siteid

	where eventtypeid = 1 and eventdatedefaulttimezone::date between '2018-09-01' and '2019-02-19'
	      and (advertiserid != 0)
-- 	and t1.campaignid in (908875, 912805, 913509, 913772, 913792, 913795, 914697, 939667, 950456, 955274)
	      and (t1.campaignid =913795 or t1.campaignid =913509)
    and t1.siteid is not null

-- 	group by eventdatedefaulttimezone,
-- 	         userid,
-- 	         campaignid
) t1



group by
	       eventdatedefaulttimezone::date,
-- 	       userid,
--            t1.sitename,
-- 	       t1.siteid
--          ,
           t1.campaignname,
	       t1.campaignid

-- ===================================================================================================================
-- FY18 Detail

select
	       eventdatedefaulttimezone::date,
-- 	       userid,
--            t1.sitename,
-- 	       t1.siteid,
           t1.campaignname,
	       t1.campaignid,
           count(distinct userid) as users,
		   sum(imps) as imps


from
(
	select
	       eventdatedefaulttimezone,
	       userid,
           s1.sitename,
	       t1.siteid,
	       c1.campaignname,
	       t1.campaignid,
	       1 as imps

	from wmprodfeeds.ikea.sizmek_standard_events t1

	left join wmprodfeeds.ikea.sizmek_display_campaigns c1
on t1.campaignid = c1.campaignid

		left join wmprodfeeds.ikea.sizmek_sites s1
	on t1.siteid = s1.siteid

	where eventtypeid = 1 and eventdatedefaulttimezone::date between '2017-09-01' and '2018-07-31'
	      and (advertiserid != 0)
-- 	and campaignid in (908875, 912805, 913509, 913772, 913792, 913795, 914697, 939667, 950456, 955274)
    and t1.campaignid =813500
    and t1.siteid is not null

-- 	group by eventdatedefaulttimezone,
-- 	         userid,
-- 	         campaignid
) t1



group by
	       eventdatedefaulttimezone::date,
-- 	       userid,
--            t1.sitename,
-- 	       t1.siteid
--          ,
           t1.campaignname,
	       t1.campaignid

-- ===================================================================================================================
-- FY18 Detail

select

t3.mm_dd,
t3.device,
avg(reach) as avg_reach


from (
       select
              t2.mm_dd,
t2.sitename,
t2.target,
t2.objective,
t2.media,
t2.funnel,
t2.device,
              sum(users) as users,
              sum(imps) as imps,
       sum(cast(users as decimal(20,10)))/sum(cast(imps as decimal(20,10))) as reach

from (
    select
	       eventdatedefaulttimezone::date as mm_dd,
-- 	       userid,
           t1.sitename,
       t1.target,
       t1.objective,
      t1.media,
      t1.funnel,
      t1.device,
-- 			t1.placementname,
-- 	       t1.siteid,
--            t1.campaignname,
-- 	       t1.campaignid,
           count(distinct userid) as users,
		   sum(imps) as imps
--           count(distinct userid)/sum(imps) as reach


from
(
	select
	       eventdatedefaulttimezone,
	       userid,
           s1.sitename,
	       t1.siteid,
	       p1.placementname,
-- SPLIT_PART(p1.placementname, '_', 5) as target,
	       	       decode(SPLIT_PART(p1.placementname, '_', 3),

			'E', 'Engagements',
			'NA', 'NA',
			'NC', 'Online Conversions',
			'AW', 'Awareness',
			'ITS', 'ITS Actions'

			) as objective,


	       decode(SPLIT_PART(p1.placementname, '_', 5),
			'C1', 'Contextual',
			'D1', 'Demo',
			'B1', 'Behavioral',
			'G1', 'Contextual',
			'L1', 'Look a Like',
			'R1', 'Retargeting',
			'P1', 'Prospecting',
			'C2', 'Competitive Conq'

			) as target,

	       decode(SPLIT_PART(p1.placementname, '_', 8),
			'GL' ,'Lower',
			'GU' ,'Upper',
			'HL' ,'Lower',
			'HU' ,'Upper'

			) as funnel,

	       decode(SPLIT_PART(SPLIT_PART(p1.placementname, '_', 9),'q',2),
			'NAT', 'Nat',
			'AUD', 'Audio',
			'BAN', 'Banner',
			'RM', 'Rich Mecia',
			'PRE', 'Pre-Roll',
			'HPT', 'Homepage Takeover',
			'VID', 'Video'
			) as media,

	       decode(SPLIT_PART(SPLIT_PART(p1.placementname, '_', 9),'q',1),
			'M', 'Mobile',
			'C', 'Cross Device',
			'D', 'Desktop',
			'T', 'Tablet',
			'V', 'CTV'
			) as device,

	       c1.campaignname,
	       t1.campaignid,
	       1 as imps

	from wmprodfeeds.ikea.sizmek_standard_events t1

	left join wmprodfeeds.ikea.sizmek_display_campaigns c1
	on t1.campaignid = c1.campaignid

	left join wmprodfeeds.ikea.sizmek_sites s1
	on t1.siteid = s1.siteid

	left join wmprodfeeds.ikea.sizmek_placements p1
	on t1.placementid = p1.placementid

	where eventtypeid = 1
--     and eventdatedefaulttimezone::date between '2018-09-01' and '2019-02-19'
    and eventdatedefaulttimezone::date between '2018-09-01' and '2019-02-19'
	      and (advertiserid != 0)
-- 	and campaignid in (908875, 912805, 913509, 913772, 913792, 913795, 914697, 939667, 950456, 955274)
    and (t1.campaignid =913795 or t1.campaignid =913509)
    and t1.siteid is not null
    and t1.siteid not in (138700, 112714)


-- 	group by eventdatedefaulttimezone,
-- 	         userid,
-- 	         campaignid
) t1



group by
--          t1.placementname
           eventdatedefaulttimezone::date,
       t1.target,
       t1.objective,
      t1.media,
      t1.funnel,
      t1.device,
-- -- 	       userid,
            t1.sitename
-- -- 	       t1.siteid
-- --          ,
--            t1.campaignname,
-- 	       t1.campaignid
	) t2

group by
t2.mm_dd,
t2.sitename,
t2.target,
t2.objective,
t2.media,
t2.funnel,
t2.device


	       ) t3
	       group by
t3.mm_dd,
t3.device;