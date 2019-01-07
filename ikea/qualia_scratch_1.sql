
select
        t3.date,
                t3.campaign,
                sum(t3.imps)        as imps,
        sum(t3.clks)        as clks,
                sum(t3.total_conv)  as total_conv,
        sum(t3.quant)       as quant,
                sum(t3.stock_prog)  as stock_prog,
        sum(t3.email_list)  as email_list,
                sum(t3.stock_chck)  as stock_chck,
        sum(t3.email_list)  as print_list

from
   (select
                                t1.eventdatedefaulttimezone::date as date,
--                              t1.userid,
                              case when t1.campaignid in (701852, 701863, 704173, 712114) then '2017' else '2018' end as campaign,
                          sum(case when t1.eventtypeid = 1 then 1 else 0 end) as imps,
                              sum(case when t1.eventtypeid = 2 then 1 else 0 end) as clks,
                0 as total_conv,
                0 as quant,
                0 as stock_prog,
                0 as email_list,
                0 as stock_chck,
                0 as print_list

                from
                                wmprodfeeds.ikea.sizmek_standard_events    as t1
                where
                t1.campaignid in (701852, 701863, 704173, 712114, 813500) and
                                t1.eventtypeid in (1,2) and      --impression
                                t1.userid != '0' and
                            cast(t1.eventdatedefaulttimezone as date) between '2017-01-01' and '2018-08-31'
            group by
                    t1.eventdatedefaulttimezone::date,
                    t1.campaignid
               union all

                select
                        t2.winnereventdatedefaulttimezone::date as date,
--                                 count(distinct t2.userid) as users,
                              case when t2.campaignid in (701852, 701863, 704173, 712114) then '2017' else '2018' end as campaign,
                                0 as imps,
                                0 as clks,
                        count(*) as total_conv,
                        sum(t2.quantity) as quant,
                        sum(case when t2.conversiontagid = 1061036 then 1 else 0 end) as stock_prog,
                        sum(case when t2.conversiontagid = 612254  then 1 else 0 end) as email_list,
                        sum(case when t2.conversiontagid = 596537  then 1 else 0 end) as stock_chck,
                        sum(case when t2.conversiontagid = 612256  then 1 else 0 end) as print_list

                from
                                wmprodfeeds.ikea.sizmek_conversion_events      as t2
                where
                t2.campaignid in (701852, 701863, 704173, 712114, 813500) and
--                                 t2.userid != '0' and
                t2.isconversion = 'true' and
                cast(t2.winnereventdatedefaulttimezone as date) between '2017-01-01' and '2018-08-31'
            group by
                    t2.winnereventdatedefaulttimezone::date,
                    t2.campaignid
                ) as t3
-- left join wmprodfeeds.ikea.sizmek_display_campaigns as c1
--     on t3.campaignid = c1.campaignid
group by
        t3.date,
                t3.campaign

    ;



select conversionname, conversiontagid from wmprodfeeds.ikea.sizmek_conversions_tags
where conversionname in ( 'ike_stockprognosis_2017 ',
 'ITS - Stock Prognosis ',
 'ITS Prognosis ',
 'Prognosis ',
 'Stock Prognosis ',
 'FY15_IKE_LSP_Tempe_PageLoad_03062015 ',
 'FY15_IKE_LSP_Burbank_PageLoad_03062015 ',
 'FY15_IKE_LSP_Carson_PageLoad_03062015 ',
 'FY15_IKE_LSP_CostaMesa_PageLoad_03062015 ',
 'FY15_IKE_LSP_Covina_PageLoad_03062015 ',
 'FY15_IKE_LSP_EastPaloAlto_PageLoad_03062015 ',
 'FY15_IKE_LSP_Emeryville_PageLoad_03062015 ',
 'FY15_IKE_LSP_SanDiego_PageLoad_03062015 ',
 'FY15_IKE_LSP_WestSacramento_PageLoad_03062015 ',
 'FY15_IKE_LSP_Centennial_PageLoad_03062015 ',
 'FY15_IKE_LSP_NewHaven_PageLoad_03062015 ',
 'FY15_IKE_LSP_Miami_PageLoad_03062015 ',
 'FY15_IKE_LSP_Orlando_PageLoad_03062015 ',
 'FY15_IKE_LSP_Sunrise_PageLoad_03062015 ',
 'FY15_IKE_LSP_Tampa_PageLoad_03062015 ',
 'FY15_IKE_LSP_Atlanta_PageLoad_03062015 ',
 'FY15_IKE_LSP_Bolingbrook_PageLoad_03062015 ',
 'FY15_IKE_LSP_Schaumburg_PageLoad_03062015 ',
 'FY15_IKE_LSP_Merriam_PageLoad_03062015 ',
 'FY15_IKE_LSP_Stoughton_PageLoad_03062015 ',
 'FY15_IKE_LSP_Baltimore_PageLoad_03062015 ',
 'FY15_IKE_LSP_CollegePark_PageLoad_03062015 ',
 'FY15_IKE_LSP_Canton_PageLoad_03062015 ',
 'FY15_IKE_LSP_TwinCities_PageLoad_03062015 ',
 'FY15_IKE_LSP_StLouis_PageLoad_03062015 ',
 'FY15_IKE_LSP_Charlotte_PageLoad_03062015 ',
 'FY15_IKE_LSP_Elizabeth_PageLoad_03062015 ',
 'FY15_IKE_LSP_Paramus_PageLoad_03062015 ',
 'FY15_IKE_LSP_Brooklyn_PageLoad_03062015 ',
 'FY15_IKE_LSP_LongIsland_PageLoad_03062015 ',
 'FY15_IKE_LSP_WestChester_PageLoad_03062015 ',
 'FY15_IKE_LSP_Portland_PageLoad_03062015 ',
 'FY15_IKE_LSP_Conshohocken_PageLoad_03062015 ',
 'FY15_IKE_LSP_Pittsburgh_PageLoad_03062015 ',
 'FY15_IKE_LSP_SouthPhiladelphia_PageLoad_03062015 ',
 'FY15_IKE_LSP_Dallas_PageLoad_03062015 ',
 'FY15_IKE_LSP_Houston_PageLoad_03062015 ',
 'FY15_IKE_LSP_RoundRock_PageLoad_03062015 ',
 'FY15_IKE_LSP_Draper_PageLoad_03062015 ',
 'FY15_IKE_LSP_Woodbridge_PageLoad_03062015 ',
 'FY15_IKE_LSP_Seattle_PageLoad_03062015 ',
 'FY15_IKE_LSP_LasVegas_PageLoad_01192016 ',
 'FY17_IKE_LSP_Memphis_PageLoad_10182016 ',
 'IKE_LSP_HMBurbank_PageLoad_2017 ',
 'FY17_IKE_LSP_Columbus_PageLoad_08282017 ',
 'FY17_IKE_LSP_Fishers_PageLoad_ ',
 'FY18_IKE_LSP_Jacksonville_PageLoad_10.23.17 ',
 'FY18_IKE_LSP_GrandPrairie_PageLoad_10.27.17 ',
 'FY18_IKE_LSP_Oak Creek_PageLoad_04218 ',
 'FY15_IKE_StoreLocator_SalesTagButton_2182015 ',
 'FY15_IKE_EmailShoppingList_SalesTagButton_4102015 ',
 'FY15_IKE_PrintShoppingList_SalesTagButton_4102015 ',
 'Local Store Pages ',
 'NEW Local Store Page ',
 'ITS - Email Shopping List ',
 'ITS - Local Store Page ',
 'ITS - Print Shopping List ',
 'ITS Email Shopping List ',
 'ITS Local Store Page ',
 'ITS Print Shopping List ',
 'ITS - Store Locator ')


(593715, 598734, 598735, 598736, 598737, 598738, 598739, 598740, 598741, 598742, 598743, 598744, 598745, 598746, 598747, 598748, 598749, 598750, 598751, 598752, 598753, 598754, 598755, 598756, 598757, 598758, 598759, 598760, 598761, 598762, 598763, 598764, 598765, 598766, 598767, 598768, 598769, 598770, 598771, 598772, 598773, 598774, 612254, 612256, 736352, 930091, 1018765, 1076168, 1084971, 1098595, 1100473, 1174087)

select * from wmprodfeeds.ikea.sizmek_display_campaigns
      where campaignname = 'IKE_IKE_SEA_GM-Commercial_FY18_DIS_AWR_X_X_(IKE-SEA-028)'

-- ===============================================================================================

select
        t3.date,
                t3.campaign,
                sum(t3.imps)        as imps,
        sum(t3.clks)        as clks,
                sum(t3.total_conv)  as total_conv,
        sum(t3.quant)       as quant,
                sum(t3.stock_prog)  as stock_prog,
        sum(t3.email_list)  as email_list,
                sum(t3.stock_chck)  as stock_chck,
        sum(t3.email_list)  as print_list

from
   (select
                                t1.eventdatedefaulttimezone::date as date,
--                              t1.userid,
                              case when t1.campaignid in (701852, 701863, 704173, 712114) then '2017' else '2018' end as campaign,
                          sum(case when t1.eventtypeid = 1 then 1 else 0 end) as imps,
                              sum(case when t1.eventtypeid = 2 then 1 else 0 end) as clks,
                0 as total_conv,
                0 as quant,
                0 as stock_prog,
                0 as email_list,
                0 as stock_chck,
                0 as print_list

                from
                                wmprodfeeds.ikea.sizmek_standard_events    as t1
                where
                t1.campaignid in (701852, 701863, 704173, 712114, 813500) and
                                t1.eventtypeid in (1,2) and      --impression
                                t1.userid != '0' and
                            cast(t1.eventdatedefaulttimezone as date) between '2017-01-01' and '2018-08-31'
            group by
                    t1.eventdatedefaulttimezone::date,
                    t1.campaignid
               union all

                select
                        t2.winnereventdatedefaulttimezone::date as date,
--                                 count(distinct t2.userid) as users,
                              case when t2.campaignid in (701852, 701863, 704173, 712114) then '2017' else '2018' end as campaign,
                                0 as imps,
                                0 as clks,
                        count(*) as total_conv,
                        sum(t2.quantity) as quant,
                        sum(case when t2.conversiontagid = 1061036 then 1 else 0 end) as stock_prog,
                        sum(case when t2.conversiontagid = 612254  then 1 else 0 end) as email_list,
                        sum(case when t2.conversiontagid = 596537  then 1 else 0 end) as stock_chck,
                        sum(case when t2.conversiontagid = 612256  then 1 else 0 end) as print_list

                from
                                wmprodfeeds.ikea.sizmek_conversion_events      as t2
                where
                t2.campaignid in (701852, 701863, 704173, 712114, 813500) and
--                                 t2.userid != '0' and
                t2.isconversion = 'true' and
                cast(t2.winnereventdatedefaulttimezone as date) between '2017-01-01' and '2018-08-31'
            group by
                    t2.winnereventdatedefaulttimezone::date,
                    t2.campaignid
                ) as t3
-- left join wmprodfeeds.ikea.sizmek_display_campaigns as c1
--     on t3.campaignid = c1.campaignid
group by
        t3.date,
                t3.campaign

    ;

-- ===============================================================================================


select
        t3.userid,
            sum(imps) as imps,
            sum(its_con) as its_con

from
   (select
                    userid,
--                                 t1.eventdatedefaulttimezone::date as date,
--                              t1.userid,
--                          sum(case when t1.eventtypeid = 1 then 1 else 0 end) as imps,
--                              sum(case when t1.eventtypeid = 2 then 1 else 0 end) as clks,
                    sum(1) as imps,
                    0 as its_con
--                0 as quant,
--                0 as stock_prog,
--                0 as email_list,
--                0 as stock_chck,
--                0 as print_list

                from
                                wmprodfeeds.ikea.sizmek_standard_events    as t1
                where
--                t1.campaignid in (701852, 701863, 704173, 712114, 813500) and
                                t1.eventtypeid = 1 and      --impression
                                t1.userid != '0' and
                            cast(t1.eventdatedefaulttimezone as date) between '2017-09-01' and '2018-08-31'
            group by
                    userid
               union all

                select

                            userid,
                                0 as imps,
                        sum(case when t2.conversiontagid in ((593715, 598734, 598735, 598736, 598737, 598738, 598739, 598740, 598741, 598742, 598743, 598744, 598745, 598746, 598747, 598748, 598749, 598750, 598751, 598752, 598753, 598754, 598755, 598756, 598757, 598758, 598759, 598760, 598761, 598762, 598763, 598764, 598765, 598766, 598767, 598768, 598769, 598770, 598771, 598772, 598773, 598774, 612254, 612256, 736352, 930091, 1018765, 1076168, 1084971, 1098595, 1100473, 1174087)) then 1 else 0 end) as its_con

                from
                                wmprodfeeds.ikea.sizmek_conversion_events      as t2
                where
--                t2.campaignid in (701852, 701863, 704173, 712114, 813500) and
                                t2.userid != '0' and
                t2.isconversion = 'true' and
                cast(t2.winnereventdatedefaulttimezone as date) between '2017-09-01' and '2018-08-31'
            group by
                    userid
                ) as t3
-- left join wmprodfeeds.ikea.sizmek_display_campaigns as c1
--     on t3.campaignid = c1.campaignid
group by
        userid

    ;

-- ===============================================================================================


