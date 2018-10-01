create procedure dbo.crt_sch_keywordTbl_3
as
  if OBJECT_ID('master.dbo.sch_keyword_3',N'U') is not null
    drop table master.dbo.sch_keyword_3;
  create table dbo.sch_keyword_3
  (
    paid_search_keyword_id  bigint,
    paid_search_keyword     varchar(1000)
  );

  insert into dbo.sch_keyword_3

    select
      t1.paid_search_keyword_id,
      t1.paid_search_keyword
    from (
           select
      isNull(cast(PdSearch_Keyword_ID as bigint),cast(0 as bigint)) as paid_search_keyword_id,
             cast(PdSearch_Keyword as varchar(1000)) as paid_search_keyword
           from [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_Search_Floodlight

           group by
            cast(PdSearch_Keyword_ID as bigint),
             cast(PdSearch_Keyword as varchar(1000))
           union all

           select
      isNull(cast(paid_search_keyword_id as bigint),cast(0 as bigint)) as paid_search_keyword_id,
             cast(paid_search_keyword as varchar(1000)) as paid_search_keyword
           from [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchKeyword

           group by
            cast(paid_search_keyword_id as bigint),
             cast(paid_search_keyword as varchar(1000))
           union all

           select *
           from openQuery(verticaunited,
                          'select isNull(cast(paid_search_keyword_id as bigint),cast(0 as bigint)) as Paid_Search_Keyword_ID, cast(paid_search_keyword as varchar(1000)) as paid_search_keyword   from diap01.mec_us_united_20056.dfa2_paid_search
           group by
           cast(paid_search_keyword_id as bigint),
             cast(paid_search_keyword as varchar(1000))'

           )

                   union all

           select *
           from openQuery(verticaunited,
                         'select isNull(cast(extended_keyword_id as bigint),cast(0 as bigint)) as Paid_Search_Keyword_ID, cast(keyword_name as varchar(1000)) as paid_search_keyword   from diap01.mec_us_united_20056.dfa_dartsearch
           group by
           cast(extended_keyword_id as bigint),
             cast(keyword_name as varchar(1000))'

           )

         ) as t1

      where t1.paid_search_keyword_id <> 0

    group by
      t1.paid_search_keyword_id,
      t1.paid_search_keyword