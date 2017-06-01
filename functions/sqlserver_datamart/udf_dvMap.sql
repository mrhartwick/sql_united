/*
This function works on the dv_map field, which is maintained by Planning/Investment and indicates whether a
placement is subject to viewability. "N" means not subject to viewability; "Y" means subject, data sourced
from DoubleVerify; "M" means subject, data sourced from Moat.

This logic does two things:
1.) corrects known mistakes in the dv_map field
2.) corrects unknown but foreseeable mistakes, e.g. dv_map = "N" but the placement name has "DV" in it.
*/

alter function [dbo].udf_dvMap(
    @campaign_id int, @site_id_dcm int, @placement nvarchar(4000), @cost_method nvarchar(100), @dv_map nvarchar(1)
)
    returns varchar(1)
    with execute as caller
as
    begin
        declare @final_dv_map varchar(4000)
        set @final_dv_map = case
               -- These cost methods should never be subject to viewability
               when @cost_method = 'Flat' or @cost_method = 'CPC' or @cost_method = 'CPCV' or @cost_method = 'dCPM'
                   then 'N'

               -- Corrections to SME 2016
               when @campaign_id = '10090315' and
                   (@site_id_dcm = '1513807' or   -- Inc
                    @site_id_dcm = '1592652')     -- Xaxis
                   then 'Y'

          -------Correction to Trade 2017, Vistar Media; MOAT tag not working, so using DV

         when @campaign_id = '11069476' and
                   (@site_id_dcm = '2244596') ---Vistar Media
                   then 'Y'

          ----- Corrections to SFO-SIN 2016
               when @campaign_id = '9923634' and
                  ((@site_id_dcm = '1534879' and @cost_method = 'CPM') or   -- Business Insider
                   (@site_id_dcm = '1853564'))                                -- Live Intent
                   then 'N'

               -- FlipBoard unable to implement Moat tags; must bill off of DFA impressions
               when @site_id_dcm = '2937979' then 'N'
               -- All targeted marketing subject to viewability; mark "Y"
               when @campaign_id = '9639387' then 'Y'

               -- If it's CPMV and the placement has these words, then it should be in Moat
               when @cost_method = 'CPMV' and
                   (@placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%' or
                    @placement like '%[Vv][Ii][Dd][Ee][Oo]%' or
                    @placement like '%[Pp][Rr][Ee]%[Rr][Oo][Ll][Ll]%' or
                    @site_id_dcm = '1995643'  -- Verve
               ) then 'M'

               -- Look for viewability flags Investment began to include in placement names 6/16.
               when @placement like '%[_]DV[_]%' then 'Y'
               when @placement like '%[_]MOAT[_]%' then 'M'
               when @placement like '%[_]NA[_]%' then 'N'

               -- If it's CPMV and marked "N," change to "Y"
               when @cost_method = 'CPMV' and @dv_map = 'N' then 'Y'
               else @dv_map end;
        return @final_dv_map
    end
go
