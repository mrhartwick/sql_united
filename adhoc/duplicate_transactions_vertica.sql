/* Query finds all duplicate ticket confirmation events in the month of September */
/* The entire conversion record will likely not be duplicated exactly but the     */
/* records will have the same PNR, Revenue, Quantity and Origin/Destination pair  */
SELECT   SubString(Other_Data,instr('u18=',Other_Data)+4,8)  AS Base64PNR
        ,*
FROM     mec.UnitedUS.dfa_activity
WHERE    cast(activity_time as date) BETWEEN '8/1/2016' AND '8/30/2016'
AND      SubString(Other_Data,instr('u18=',Other_Data)+4,8)
IN      (SELECT   A.Base64PNR
         FROM    (SELECT   SubString(Other_Data,instr('u18=',Other_Data)+4,8) AS Base64PNR
                          ,Sum(1)                                                 AS PNR_Count
                  FROM     mec.UnitedUS.dfa_activity
                  WHERE    Revenue  > 0
                  AND      Quantity > 0
                  AND      Activity_Type     = 'ticke498'                          -- Ticket Purchase
                  AND      Activity_Sub_Type = 'unite820'                          -- on United.com
                  AND      cast(activity_time as date) BETWEEN '8/1/2016' AND '8/30/2016' -- September
                  AND      instr('stage.united.com',Other_Data) = 0            -- Not Staging
                  AND      instr('qa.united.com',Other_Data)    = 0            ---Not QA
                  GROUP BY SubString(Other_Data,instr('u18=',Other_Data)+4,8))AS A
         WHERE    A.PNR_Count > 1)
ORDER BY SubString(Other_Data,instr('u18=',Other_Data)+4,8)
        ,activity_time;

-- 24,187 Rows | 00:05:33