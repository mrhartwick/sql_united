select
        t11.path,
        count(user_id) as users,
--         sum(t11.path_rank),
        sum(t11.con) as con,
--         sum(t11.tix) as tix,
        sum(t11.rev) as rev,
        sum(t11.imp) as imp

from (
        select
                t10.user_id,
                t10.path,
                t10.con,
--                 t10.tix,
                t10.rev,
                t10.imp
--                 row_number() over (partition by t10.user_id,t10.path order by t10.user_id) as path_rank
--         from (
--                 select
--                         t91.user_id,
--                         t91.path,
--                         sum(j4.con) as con,
--                         sum(j4.tix) as tix,
--                         sum(j4.rev) as rev,
--                         sum(t91.imp) as imp

                from
                                (
                        select
                                t9.user_id,
                                t9.imp,
                                t9.con,
                                t9.rev,
                                t9.path_1 || '->' || t9.path_2 || '->' || t9.path_3  as path

                        from (
                                select
                                t8.user_id,
                                t8.imp,
                                    t8.con,
                                    t8.rev,
                                case when t8.path_1 = 1    then 'Adara-prs'
                                when t8.path_1 = 2         then 'Amobee-prs'
                                when t8.path_1 = 3         then 'Google-rtg'
                                when t8.path_1 = 4         then 'QuantCast-prs'
                                when t8.path_1 = 5         then 'Sojern-prs'
                                when t8.path_1 = 6         then 'Sojern-rtg'          else 'xxx' end as path_1,

                                case when t8.path_2 = 1    then 'Adara-prs'
                                when t8.path_2 = 2         then 'Amobee-prs'
                                when t8.path_2 = 3         then 'Google-rtg'
                                when t8.path_2 = 4         then 'QuantCast-prs'
                                when t8.path_2 = 5         then 'Sojern-prs'
                                when t8.path_2 = 6         then 'Sojern-rtg'   else 'xxx' end as path_2,

                                case when t8.path_3 = 1    then  'Adara-prs'
                                when t8.path_3 = 2         then  'Amobee-prs'
                                when t8.path_3 = 3         then  'Google-rtg'
                                when t8.path_3 = 4         then  'QuantCast-prs'
                                when t8.path_3 = 5         then  'Sojern-prs'
                                when t8.path_3 = 6         then  'Sojern-rtg'    else 'xxx' end as path_3

                                from (

                                select
                                        t7.user_id,
                                        sum(t7.path_1) as path_1,
                                        sum(t7.path_2) as path_2,
                                        sum(t7.path_3) as path_3,
                                        count(distinct t7.imp_nbr) as imp,
                                        count(distinct t7.cvr_nbr) as con,
                                        sum(t7.rev) as rev
                                        from (
                                        select
                                                t6.user_id,
                                                t6.imp_nbr,
                                                t6.cvr_nbr,
                                                t6.rev,

                                                case
                                                when path_1 = 'Adara-prs'   then 1
                                                when path_1 = 'Amobee-prs'   then 2
                                                when path_1 = 'Google-rtg'   then 3
                                                when path_1 = 'QuantCast-prs'   then 4
                                                when path_1 = 'Sojern-prs'   then 5
                                                when path_1 = 'Sojern-rtg'   then 6
                                                else 0 end as path_1,

--                                                 case when regexp_like(t6.path_1,'adar.*','ib') then 1
--                                                 when regexp_like(t6.path_1,'amobee.*','ib') then 2
--                                                 when regexp_like(t6.path_1,'google.*','ib') then 3
--                                                 when regexp_like(t6.path_1,'internet.*','ib') then 4
--                                                 when regexp_like(t6.path_1,'travel.*','ib') then 5
--                                                 when regexp_like(t6.path_1,'viant.*','ib') then 6
--                                                 when regexp_like(t6.path_1,'xaxis.*','ib') then 7
--                                                 else 0 end as path_1,

--                                                 case
--                                                 when regexp_like(t6.path_2,'adar.*','ib') then 1
--                                                 when regexp_like(t6.path_2,'amobee.*','ib') then 2
--                                                 when regexp_like(t6.path_2,'google.*','ib') then 3
--                                                 when regexp_like(t6.path_2,'internet.*','ib') then 4
--                                                 when regexp_like(t6.path_2,'travel.*','ib') then 5
--                                                 when regexp_like(t6.path_2,'viant.*','ib') then 6
--                                                 when regexp_like(t6.path_2,'xaxis.*','ib') then 7
--                                                 else 0 end as path_2,

                                                case
                                                when path_2 = 'Adara-prs'   then 1
                                                when path_2 = 'Amobee-prs'   then 2
                                                when path_2 = 'Google-rtg'   then 3
                                                when path_2 = 'QuantCast-prs'   then 4
                                                when path_2 = 'Sojern-prs'   then 5
                                                when path_2 = 'Sojern-rtg'   then 6
                                                else 0 end as path_2,

--                                                 case
--                                                 when regexp_like(t6.path_3,'adar.*','ib') then 1
--                                                 when regexp_like(t6.path_3,'amobee.*','ib') then 2
--                                                 when regexp_like(t6.path_3,'google.*','ib') then 3
--                                                 when regexp_like(t6.path_3,'internet.*','ib') then 4
--                                                 when regexp_like(t6.path_3,'travel.*','ib') then 5
--                                                 when regexp_like(t6.path_3,'viant.*','ib') then 6
--                                                 when regexp_like(t6.path_3,'xaxis.*','ib') then 7
--                                                 else 0 end as path_3

                                                case
                                                when path_3 = 'Adara-prs'   then 1
                                                when path_3 = 'Amobee-prs'   then 2
                                                when path_3 = 'Google-rtg'   then 3
                                                when path_3 = 'QuantCast-prs'   then 4
                                                when path_3 = 'Sojern-prs'   then 5
                                                when path_3 = 'Sojern-rtg'   then 6
                                                else 0 end as path_3


                                                from (
                                                select

                                                        --                                       t5.site_dcm,
                                                        t5.user_id,
--                                                         t5.tp_rank,
                                                        t5.imp_nbr,
                                                        t5.cvr_nbr,
                                                        t5.rev,

                                                        case when t5.tp_rank = 3 then t5.thing else 'zzz' end as path_1,
                                                        case when t5.tp_rank = 2 then t5.thing else 'zzz' end as path_2,
                                                        case when t5.tp_rank = 1 then t5.thing else 'zzz' end as path_3
-- select distinct t5.thing
                                                        from
	                                                        ( select
		                                                        t4.user_id,
		                                                        t4.tp_rank,
                                                                t4.type,
                                                                t4.site_dcm,
-- 		                                                        count(distinct t4.imp_nbr) as imp_nbr,
-- 		                                                        count(distinct t4.cvr_nbr) as cvr_nbr,
-- 		                                                        sum(t4.rev) as rev,
                                                                t4.imp_nbr as imp_nbr,
		                                                        t4.cvr_nbr as cvr_nbr,
		                                                        t4.rev as rev,
			                                                    t4.site_dcm || '-' || t4.type as thing
		                                                       from  diap01.mec_us_united_20056.ual_acq_exposure_3  as t4
			                                                       where t4.tp_rank < 4
--                                                                 group by
--     		                                                        t4.user_id,
-- 		                                                        t4.tp_rank,
--                                                                 t4.type,
--                                                                 t4.site_dcm
                                                        ) as t5

                                                ) as t6
                                        ) as t7
                                        group by
                                        t7.user_id

                                ) as t8
                        ) as t9
--                 ) as t91
--                 left join (select
--                 user_id                               as user_id,
--                 count(*)                              as con,
--                 sum(a1.quantity)                      as tix,
--                 sum(a1.revenue / rates.exchange_rate) as rev
--
--                 from
--                 (
--                 select *
--                 from diap01.mec_us_united_20056.dfa2_activity
--                 where (cast(click_time as date) between '2017-08-15' and '2017-12-31')
--                 and advertiser_id <> 0
--                 and revenue <> 0
--                 and quantity <> 0
--                 and (activity_type = 'ticke498')
--                 and (activity_sub_type = 'unite820')
--                 and advertiser_id <> 0
--                 and order_id = 9639387
--                 and user_id <> '0'
--                 and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
--                 ) as a1
--
--                 left join diap01.mec_us_mecexchangerates_20067.EXCHANGE_RATES as rates
--                 on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
--                 and cast(a1.click_time as date) = rates.date
--
--                 group by
--
--                 a1.user_id
--
--                 ) as j4
--                 on t91.user_id = j4.user_id
--
--                 group by
--                 t91.user_id,
--                 t91.path
        ) as t10
) as t11
group by t11.path