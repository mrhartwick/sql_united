
SELECT     A.Impressions      AS Frequency
          ,Count(A.user_id)   AS Users
          ,Sum(Impressions)   AS Impressions
          ,sum(a.cost) as cost
FROM
(
           SELECT     user_id
                     ,Sum(1) AS Impressions
                     ,cast(prs.rate as decimal(10,2))/1000 as cost
           FROM       verticaunited.diap01.mec_us_united_20056.dfa2_impression as t1

      left join
      (
        select *
        from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].prs_summ
      ) as prs
        on t1.placement_id = prs.adserverplacementid

           WHERE      user_id != '0'
--         AND    cast(convert(datetime,cast(event_time/1000000 as timestamp)) as date) BETWEEN '2017-08-16' AND '2017-10-23'
           AND    cast(convert(datetime,md_event_time) as date) BETWEEN '2017-08-16' AND '2017-10-23'

--            AND        Date(TO_TIMESTAMP(event_time/1000000)) BETWEEN '2017-08-16' AND '2017-10-23'
           AND        site_id_dcm = 1190273 --Adara United
           GROUP BY
               user_id
    ,prs.rate
)          A
GROUP BY   A.Impressions
ORDER BY   A.Impressions;


-- Version without cost =======================================================================

SELECT     A.Impressions      AS Frequency
          ,Count(A.user_id)   AS Users
          ,Sum(Impressions)   AS Impressions
FROM
(
           SELECT     user_id
                     ,Sum(1) AS Impressions
           FROM       diap01.mec_us_united_20056.dfa2_impression
           WHERE      user_id != '0'

           AND        cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) BETWEEN '2017-08-16' AND '2017-10-23'
           AND        site_id_dcm = 1190273 --Adara United
           GROUP BY   user_id
)          A
GROUP BY   Frequency
ORDER BY   Frequency;
