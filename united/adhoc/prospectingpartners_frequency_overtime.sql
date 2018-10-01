SELECT     A.Impressions      AS Frequency
          ,Count(A.user_id)   AS Users
          ,Sum(Impressions)   AS Impressions
FROM
(
           SELECT     user_id
                     ,Sum(1) AS Impressions
           FROM       diap01.mec_us_united_20056.dfa2_impression
           WHERE      user_id != '0'
           AND        Date(TO_TIMESTAMP(event_time/1000000)) BETWEEN '2017-08-16' AND '2017-10-23'
           AND        site_id_dcm =  1239319 ---Sojern
           GROUP BY   user_id
)          A
GROUP BY   Frequency
ORDER BY   Frequency;

--------------------------------------------

SELECT     A.Impressions      AS Frequency
          ,Count(A.user_id)   AS Users
          ,Sum(Impressions)   AS Impressions
FROM
(
           SELECT     user_id
                     ,Sum(1) AS Impressions
           FROM       diap01.mec_us_united_20056.dfa2_impression
           WHERE      user_id != '0'
           AND        Date(TO_TIMESTAMP(event_time/1000000)) BETWEEN '2017-08-16' AND '2017-10-23'
           AND        site_id_dcm =  1853562 ---Amobee
           GROUP BY   user_id
)          A
GROUP BY   Frequency
ORDER BY   Frequency;

--------------------------------------------

SELECT     A.Impressions      AS Frequency
          ,Count(A.user_id)   AS Users
          ,Sum(Impressions)   AS Impressions
FROM
(
           SELECT     user_id
                     ,Sum(1) AS Impressions
           FROM       diap01.mec_us_united_20056.dfa2_impression
           WHERE      user_id != '0'
           AND        Date(TO_TIMESTAMP(event_time/1000000)) BETWEEN '2017-08-16' AND '2017-10-23'
           AND        site_id_dcm = 3267410 ---Quantcast
           GROUP BY   user_id
)          A
GROUP BY   Frequency
ORDER BY   Frequency;


