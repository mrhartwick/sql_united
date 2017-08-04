--====================================================================
-- Author:    Matthew Hartwick
-- Create date:      04 Aug 2017 11:47:30 AM ET
-- Description: {Description}
-- Comments:
--====================================================================
IF Object_id(N'DFID060193_FT_Summ_Extracted_Post') IS NOT NULL
  DROP TABLE [DFID060193_FT_Summ_Extracted_Post]

SELECT Cast(Getdate() - 1 AS DATE) AS [Date],
       t1.*
INTO   [DFID060193_FT_Summ_Extracted_Post]
FROM   #DT t1