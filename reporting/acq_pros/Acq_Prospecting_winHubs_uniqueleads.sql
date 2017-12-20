select
cast(r1.date as date)                     as dcmdate,
campaign.campaign                         as campaign,
r1.campaign_id                            as campaign_id,
r1.site_id_dcm                            as site_id_dcm,
directory.site_dcm                        as site_dcm,
left(p1.placement,6)                      as plce_id,
replace(replace(p1.placement ,',', ''),'"','') as placement,
r1.placement_id                           as placement_id,
r1.city as city,
r1.Reg_Unreg as Reg_Unreg,
sum(r1.vew_led)                           as vew_led,
sum(r1.vew_qual_led)                      as vew_qual_led,
sum(r1.clk_led)                           as clk_led,
sum(r1.clk_qual_led)                      as clk_qual_led,
sum(r1.vew_led) + sum(r1.clk_led)         as led,
sum(r1.vew_qual_led) + sum(r1.clk_qual_led)         as qual_led
from (


select
cast (timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),'SS') as date ) as "date"
,case
when ta.placement_id in (206798246,206798393,206798399,206831200,206831218,206831614,206831647,206831659,206831668,206832295,206856135,206856138,206856174,206856177,207902802,207902805,207828554,207827324,207827939,207881356,207828551) then 'DEN'
when ta.placement_id in (206831191,206831209,206831212,206831653,206831656,206832280,206832286,206832289,206832292,206855814,206855817,206856147,206856153,206856159,206856165,207881353,207828545,207902301,207881350,207902787,207827321,207881728) then 'EWR'
when ta.placement_id in (206798249,206798369,206798375,206831605,206831638,206832265,206832274,206855790,206855796,206855799,206855802,206855808,206856141,206856150,207902808,207881359,207828557,207828542,207828548,207881734,207881725) then 'SFO' else 'Other' end as City

,case
when ta.placement_id in (206798393,206831200,206831647,206831659,206856135,206856138,206856177,206831191,206831212,206832289,206832292,206855814,206855817,206856159,206856165,206798369,206798375,206832265,206855796,206855808,206856141,206856150) then 'Registered'
when ta.placement_id in (206798246,206798399,206831218,206831614,206831668,206832295,206856174,206831209,206831653,206831656,206832280,206832286,206856147,206856153,206798249,206831605,206831638,206832274,206855790,206855799,206855802) then 'Unregistered' else 'NA' end as Reg_Unreg
,ta.campaign_id as campaign_id
,ta.site_id_dcm as site_id_dcm
,ta.placement_id as placement_id
,sum(case when activity_id = 1086066 and ta.conversion_id = 1 then 1 else 0 end) as clk_led
,sum(case when activity_id = 1086066 and (ta.conversion_id = 1) and (
(
(placement_id in (206798246,206798393,206798399,206831200,206831218,206831614,206831647,206831659,206831668,206832295,206856135,206856138,206856174,206856177,207902802,207902805,207828554,207827324,207827939,207881356,207828551)) and
(ta.orig = 'DEN')
) or
(
(placement_id in (206831191,206831209,206831212,206831653,206831656,206832280,206832286,206832289,206832292,206855814,206855817,206856147,206856153,206856159,206856165,207881353,207828545,207902301,207881350,207902787,207827321,207881728)) and
(ta.orig = 'EWR')

) or
(
(placement_id in (206798249,206798369,206798375,206831605,206831638,206832265,206832274,206855790,206855796,206855799,206855802,206855808,206856141,206856150,207902808,207881359,207828557,207828542,207828548,207881734,207881725)) and
(ta.orig = 'SFO')
)
)
then 1 else 0 end) as clk_qual_led
,sum(case when activity_id = 1086066 and ta.conversion_id = 2 then 1 else 0 end) as vew_led
,sum(case when activity_id = 1086066 and (ta.conversion_id = 2) and (
(
(placement_id in (206798246,206798393,206798399,206831200,206831218,206831614,206831647,206831659,206831668,206832295,206856135,206856138,206856174,206856177,207902802,207902805,207828554,207827324,207827939,207881356,207828551)) and
(ta.orig = 'DEN')
) or
(
(placement_id in (206831191,206831209,206831212,206831653,206831656,206832280,206832286,206832289,206832292,206855814,206855817,206856147,206856153,206856159,206856165,207881353,207828545,207902301,207881350,207902787,207827321,207881728)) and
(ta.orig = 'EWR')

) or
(
(placement_id in (206798249,206798369,206798375,206831605,206831638,206832265,206832274,206855790,206855796,206855799,206855802,206855808,206856141,206856150,207902808,207881359,207828557,207828542,207828548,207881734,207881725)) and
(ta.orig = 'SFO')
)
)
then 1 else 0 end) as vew_qual_led


from
(
select distinct(user_id), activity_id, conversion_id, interaction_time, campaign_id, site_id_dcm, placement_id,
case when REGEXP_LIKE(other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then REGEXP_SUBSTR(other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
when REGEXP_LIKE(other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') then REGEXP_SUBSTR(other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as orig,
case when REGEXP_LIKE(other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then REGEXP_SUBSTR(other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
when REGEXP_LIKE(other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib') then REGEXP_SUBSTR(other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as dest
from diap01.mec_us_united_20056.dfa2_activity
where cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date ) between '2017-10-01' and '2017-12-12'
and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
and (activity_id = 1086066)
and ((campaign_id = 20377442) or (campaign_id = 10742878 and site_id_dcm = 1190273))
and (advertiser_id <> 0)
and (length(isnull(event_sub_type,'')) > 0)
and (user_id <> '0')
) as ta


group by
cast (timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),'SS') as date )
,ta.campaign_id
,ta.site_id_dcm
,ta.placement_id



) as r1

left join
(
select cast (campaign as varchar (4000)) as 'campaign',campaign_id as 'campaign_id'
from diap01.mec_us_united_20056.dfa2_campaigns
) as campaign
on r1.campaign_id = campaign.campaign_id

left join
(
select cast (placement as varchar (4000)) as 'placement',placement_id as 'placement_id',campaign_id as 'campaign_id',site_id_dcm as 'site_id_dcm'

-- from ( select campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast (placement_start_date as date ) as thisdate,
-- row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast (placement_start_date as date ) desc ) as x1
from diap01.mec_us_united_20056.dfa2_placements

-- ) as p1
-- where x1 = 1
) as p1
on r1.placement_id    = p1.placement_id
and r1.campaign_id = p1.campaign_id
and r1.site_id_dcm  = p1.site_id_dcm

left join
(
select cast (site_dcm as varchar (4000)) as 'site_dcm',site_id_dcm as 'site_id_dcm'
from diap01.mec_us_united_20056.dfa2_sites
) as directory
on r1.site_id_dcm = directory.site_id_dcm

-- where p1.placement like '%PROS_FT%' or p1.placement like '%Weather%'
-- and r1.site_id_dcm in (1190273,1239319, 1853562, 3267410)
where ((r1.campaign_id = 20377442) or (r1.campaign_id = 10742878 and r1.site_id_dcm = 1190273 and p1.placement like '%_WinHubs_%'))

group by
cast (r1.date as date )
,directory.site_dcm
,r1.site_id_dcm
,r1.campaign_id
,r1.city
,r1.Reg_Unreg
,campaign.campaign
,r1.placement_id
,p1.placement
