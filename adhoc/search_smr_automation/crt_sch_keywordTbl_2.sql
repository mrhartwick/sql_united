alter procedure dbo.crt_sch_keywordTbl_2
as
  if OBJECT_ID('master.dbo.sch_keyword_2',N'U') is not null
    drop table master.dbo.sch_keyword_2;
  create table dbo.sch_keyword_2
  (
    paid_search_keyword_id  bigint,
    paid_search_keyword     varchar(1000)
  );

  insert into dbo.sch_keyword_2

    select
      t1.paid_search_keyword_id,
      t1.paid_search_keyword
    from (
           select
      isNull(cast(right(PdSearch_Keyword_ID, 12) as bigint),cast(0 as bigint)) as paid_search_keyword_id,
             cast(PdSearch_Keyword as varchar(1000)) as paid_search_keyword
           from [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_Search_Floodlight

           group by
            cast(right(PdSearch_Keyword_ID, 12) as bigint),
             cast(PdSearch_Keyword as varchar(1000))
           union all

           select
      isNull(cast(right(paid_search_keyword_id,12) as bigint),cast(0 as bigint)) as paid_search_keyword_id,
             cast(paid_search_keyword as varchar(1000)) as paid_search_keyword
           from [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchKeyword

           group by
            cast(right(paid_search_keyword_id,12) as bigint),
             cast(paid_search_keyword as varchar(1000))
           union all

           select *
           from openQuery(verticaunited,
                          'select isNull(cast(right(cast(paid_search_keyword_id as varchar(1000)),12) as bigint),cast(0 as bigint)) as Paid_Search_Keyword_ID, cast(paid_search_keyword as varchar(1000)) as paid_search_keyword   from diap01.mec_us_united_20056.dfa2_paid_search
           group by
           cast(right(cast(paid_search_keyword_id as varchar(1000)),12) as bigint),
             cast(paid_search_keyword as varchar(1000))'

           )

                   union all

           select *
           from openQuery(verticaunited,
                          'select isNull(cast(right(cast(extended_keyword_id as varchar(1000)), 12) as bigint),cast(0 as bigint)) as Paid_Search_Keyword_ID, cast(keyword_name as varchar(1000)) as paid_search_keyword   from diap01.mec_us_united_20056.dfa_dartsearch
           group by
           cast(right(cast(extended_keyword_id as varchar(1000)), 12) as bigint),
             cast(keyword_name as varchar(1000))'

           )

         ) as t1

      where t1.paid_search_keyword_id <> 0

    group by
      t1.paid_search_keyword_id,
      t1.paid_search_keyword

     isNull(cast(extended_keyword_id as bigint),cast(0 as bigint))
      cast(extended_keyword_id as bigint)