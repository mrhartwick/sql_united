-- select
-- count(*)
-- from
-- 	(
select
       campaign,
       aud,
--        placementname,
       funnel,
       days_to_vis,
       sitename,
       subsite,
--        siz_sid,
	   householdid

from (
select

       householdid,
       dense_rank() over (partition by householdid order by hh_imp_rank desc) as new_rank,
       impressiondate,
       visit_time,
       days_to_vis,
       funnel,
       campaign,
       siz_sid,
       sitename,
       case when sitename = 'Xaxis' then subsite || ' (xaxis)' else sitename end as subsite,
--        subsite,
--        placementname,
       hh_imp_rank,
aud
           from
(
select
       householdid,

		case when (p1.placementname like '%\\_GU\\_%' or p1.placementname like '%\\_HU\\_%') then 'upper'
		    when (p1.placementname like '%\\_GL\\_%' or p1.placementname like '%\\_HL\\_%') then 'lower'
		        else 'other' end as funnel,
--        p1.placementname,
        case
		when ((c1.campaignname = 'IKE_IKE_KIT_GM-Kitchens_FY18_DIS_AWR_X_X_(IKE-KIT-015)'
		    and (impressiondate::date between '2017-10-06' and '2017-11-19' or
		         impressiondate::date between '2018-02-28' and '2018-04-09' or
		         impressiondate::date between '2018-06-24' and '2018-07-29'
		    )
			) or (c1.campaignname = 'IKE_IKE_SEA_GM-Commercial_FY18_DIS_AWR_X_X_(IKE-SEA-028)' and (p1.placementname like '%Kitchen%' or p1.placementname like '%KITCHEN%')))
		    then 'Kitchens Offer'

        when (c1.campaignname = 'IKE_IKE_SEA_GM-Commercial_FY18_DIS_AWR_X_X_(IKE-SEA-028)') and (p1.placementname like '%\\_GU\\_%' or p1.placementname like '%\\_HU\\_%') then 'Commercial Upper'
        when (c1.campaignname = 'IKE_IKE_SEA_GM-Commercial_FY18_DIS_AWR_X_X_(IKE-SEA-028)') and (p1.placementname like '%\\_GL\\_%' or p1.placementname like '%\\_HL\\_%') then 'Commercial Lower'
		when c1.campaignname = 'IKE_IKE_KIT_GM-Kitchens_FY18_DIS_AWR_X_X_(IKE-KIT-015)' then 'Kitchens Awareness'

		when c1.campaignname = 'IKE_IKE_CTG_GM_Catalog_FY18_DIS_AWR_X_X_(IKE-CTG-002)' then 'Catalog'
		when c1.campaignname = 'IKE_IKE_GOP_OAK CREEK GO_FY18_DIS_AWR_X_X_(IKE-GOP-025)' then 'Oak Creek'
		when c1.campaignname = 'IKE_IKE_REG_GM_Registry_FY18_DIS_AWR_X_X_(IKE-REG-003)' then 'Registry'
		when c1.campaignname = 'IKE_IKE_IHT_HomeTour_FY18_DIS_AWR_X_X_(IKE-IHT-008)' then 'Home Tour'
		when c1.campaignname = 'IKE_IKE_SEG_New Movers_FY18_DIS_AWR_X_X_(IKE-SEG-015)' then 'New Movers'
		when c1.campaignname = 'IKE_IKE_GOP_GRAND PRAIRIE GO_FY18_DIS_AWR_X_X_(IKE-GOP-024)' then 'Grand Prairie'
		when c1.campaignname = 'IKE_IKE_GOP_JACKSONVILLE GO_FY18_DIS_AWR_X_X_(IKE-GOP-023)' then 'Jacksonville'
		else 'other' end as campaign,

		case when  p1.placementname like '%\\_ADS\\_%' then 'Adsmovil'
		when  p1.placementname like '%\\_BTG\\_%' then 'Batanga'
		when  p1.placementname like '%\\_BAZ\\_%' then 'BazaarVoice'
		when  p1.placementname like '%\\_EXP\\_%' then 'Experian'
		when (p1.placementname like '%\\_GGM\\_%' or  p1.placementname like '%\\_GUM\\_%' or  p1.placementname like '%\\_GUMGUM\\_%') then 'GumGum'
		when (p1.placementname like '%\\_KAR\\_%' or  p1.placementname like '%\\_KARGO\\_%') then 'Kargo'
		when  p1.placementname like '%\\_LIR\\_%' then 'Light Reaction'
		when  p1.placementname like '%\\_LIN\\_%' then 'Linqia'
		when  p1.placementname like '%\\_MAG\\_%' then 'Magnetic'
		when  p1.placementname like '%\\_MIQ\\_%' then 'Miq'
		when  p1.placementname like '%\\_OGR\\_%' then 'Ogury'
		when  p1.placementname like '%\\_OPR\\_%' then 'Oprah'
		when  p1.placementname like '%\\_OWN\\_%' then 'OWN'
		when  p1.placementname like '%\\_RF\\_%' then 'RocketFuel'
		when  p1.placementname like '%\\_STI\\_%' then 'ShareThis'
		when (p1.placementname like '%\\_SHR\\_%' or  p1.placementname like '%\\_STH\\_%' or  p1.placementname like '%\\_STR\\_%') then 'ShareThru'
		when  p1.placementname like '%\\_TAY\\_%' then 'Taykey'
		when  p1.placementname like '%\\_TDS\\_%' then 'Teads'
		when (p1.placementname like '%\\_TRE\\_%' or  p1.placementname like '%\\_TRM\\_%') then 'Tremor'
		when  p1.placementname like '%\\_TPL\\_%' then 'TripleLift'
		when  p1.placementname like '%\\_TRUEX\\_%' then 'TRUEX'
		when  p1.placementname like '%\\_UND\\_%' then 'Undertone'
		when  p1.placementname like '%\\_VER\\_%' then 'Verve'
		when  p1.placementname like '%\\_VDG\\_%' then 'Videology'
		when  p1.placementname like '%\\_XAD\\_%' then 'xAd'
		when (p1.placementname like '%\\_DM\\_%' or  p1.placementname like '%\\_DMWEB\\_%' or  p1.placementname like '%\\_X\\_%' or  p1.placementname like '%\\_XAX\\_%' or  p1.placementname like '%\\_XAXIS\\_%') then 'Xaxis'
		when  p1.placementname like '%\\_YLB\\_%' then 'Yeildbot'
		when  p1.placementname like '%\\_YMO\\_%' then 'YieldMo'
		when (p1.placementname like '%\\_YOU\\_%' or  p1.placementname like '%\\_TRUV\\_%') then 'YouTube'
		when  p1.placementname like '%\\_YUM\\_%' then 'Yume'
		else 'other' end as subsite,

       decode(SPLIT_PART(p1.placementname, '_', 13),
		'ABANDON', 'Lifestyle',
		'COMPCON', 'Compet. Conq.',
		'CROSS15', 'Lifestyle',
		'CROSS30', 'Lifestyle',
		'DEMO', 'GEN',
		'DESIGN', 'Home Design',
		'DIY', 'DIY',
		'EPISODE', 'Lifestyle',
		'FOOD', 'Food',
		'GEN', 'GEN',
		'HOMEIMP', 'Home Imp.',
		'IKEACOMP', 'Compet. Conq.',
		'IKEADMA', 'IKEA DMAs',
		'IKEADMAS', 'IKEA DMAs',
		'IKEASHOP',  'IKEA.com Shoppers',
		'IMEADMAS', 'IKEA DMAs',
		'KITCHENS', 'Lifestyle',
		'LALKITCHENS', 'Kitchens',
		'LIFESTY', 'Lifestyle',
		'LIFESTYL', 'Lifestyle',
		'LIFETSY', 'Lifestyle',
		'MOBFLEXOFF1', 'Lifestyle',
		'MOBFLEXOFF2', 'Lifestyle',
		'NEW MOVERS', 'New Movers',
		'NEWMOVE', 'New Movers',
		'PAGEGRABOFF1', 'Lifestyle',
		'PAGEGRABOFF2', 'Lifestyle',
		'PARENT', 'Baby',
		'PARENTS', 'Baby',
		'PREROLL', 'PreRoll',
		'REMODEL', 'Lifestyle',
		'SHOPPERS', 'IKEA.com Shoppers',
		'SITE', 'IKEA.com Visitors',
		'SQINTRO1', 'Lifestyle',
		'STUDENT', 'Student',
		'TRUEVIEW', 'TrueView',
		'TRUVIEW', 'TrueView',
		'WEDDING', 'Bridal', 'unknown') as aud,

       impressiondate::date,
       visit_time::date,
       days_to_vis,
       hh_imp_rank,
       siz_sid,
       case when s1.sitename = 'Well and Good NYC' then 'Well & Good' else s1.sitename end as sitename


from wmprodfeeds.ikea.qualia_visitors_hh t1

left join wmprodfeeds.ikea.sizmek_placements p1
on t1.siz_plc = p1.placementid

left join (select distinct campaignid,campaignname from wmprodfeeds.ikea.sizmek_display_campaigns) c1
on t1.siz_cid = c1.campaignid

left join wmprodfeeds.ikea.sizmek_sites s1
on t1.siz_sid = s1.siteid

where (length(isnull(store,'')) > 0)
and impressiondate is not null
and impressiondate < visit_time
and hh_vis_rank = 1
-- and hh_imp_rank = 1
and siz_plc is not null
and siz_sid is not null
and (length(isnull(s1.sitename,'')) > 0)
and s1.sitename <> 'MNI.com'
-- and siz_sid = 18556
-- Only pulling campaigns with 10K+ householdIDs
and t1.siz_cid in (
813465,
822379,
862917,
858670,
813500

	)

and impressiondate::date between '2017-09-01' and '2018-08-31'

group by
c1.campaignname,
p1.placementname,
householdid,
impressiondate::date,
visit_time::date,
days_to_vis,
         siz_sid,
         s1.sitename,
         hh_imp_rank
	) as t1
where funnel <> 'other'
-- and aud not in ('TrueView', 'Lifestyle', 'GEN', 'PreRoll', 'IKEA DMAs')
-- and aud in ('TrueView', 'Lifestyle', 'GEN', 'PreRoll', 'IKEA DMAs')
-- group by aud
	) as t2
where new_rank = 1
and subsite <> 'Linqia'
group by
       campaign,
         aud,
--          placementname,
       funnel,
         subsite,
         householdid,
         sitename,
       days_to_vis
-- 	) t3
;