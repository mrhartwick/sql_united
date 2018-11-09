WITH        impressions_set (trackable_impression, delivery_date, user_id, cookie_freq) AS
           (SELECT     CASE WHEN user_id = '0' THEN 0 ELSE 1 END        AS trackable_impression
                      ,Date(TO_TIMESTAMP(event_time/1000000))           AS delivery_date
                      ,user_id
                      ,ROW_NUMBER () OVER ( PARTITION BY user_id ORDER BY event_time ) AS cookie_freq
            FROM       mec_us_marriott_20051.MarriottDFA2_impression
            WHERE      campaign_id IN (11455516, 11389653, 10677232, 10676928)
            AND        Date(TO_TIMESTAMP(event_time/1000000)) >= '2017-06-01' )

SELECT      delivery_date
           ,Sum (Sum(trackable_impression) )
                      OVER (ORDER BY Date(delivery_date) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  )  AS trackable_impressions_to_date
           ,Sum (Sum(CASE WHEN cookie_freq * trackable_impression = 1 THEN 1 ELSE 0 END))
                      OVER (ORDER BY Date(delivery_date) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  )  AS cookies_to_date
FROM        impressions_set
GROUP BY    delivery_date
ORDER BY    delivery_date ASC;
