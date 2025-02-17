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

           ----gm acq fare sale 2018
          when @campaign_id = 20713692 then 'Y'


          ----------------------additional 2018 campaigns
          when @campaign_id = 21086860 then 'Y' --trade Jetstream
          when @campaign_id = 21206832 then 'Y'--trade rightsized
          when @campaign_id = 21415122 then 'N'--trade rightsized
          when @campaign_id = 21182874 then 'N' --economy Plus
          when @campaign_id = 21057553 then 'Y' --MileagePlus
          when @campaign_id = 21128474 then 'Y' --personal threshold
          when @campaign_id = 20945117 then 'Y' --social extension
          when @campaign_id = 21230517 and @site_id_dcm = 1190273 then 'Y' --sydney premium cabin sale
          when @campaign_id = 21434672 and @site_id_dcm = 1239319 then 'Y' --Japan Display Acq.
          when @campaign_id = 21772778 then 'Y' --iah-syd onshore
          when @campaign_id = 21822879 then 'Y' --US-Ireland DR Onshore
          when @campaign_id = 21605413 and @site_id_dcm = 4928071 then 'Y' --win sf barter
          when @campaign_id = 21852893 then 'Y' --weather test
          when @campaign_id = 21847600 then 'Y' --amazon test
          when @campaign_id = 21870944 and(@site_id_dcm = 5060516 OR @site_id_dcm = 5028818)  then 'Y' --iah-syd dr onshore foursquare nativo


--SFO-SIN 2018
          when @campaign_id = 21682827 AND (@site_id_dcm = 4167910 OR @site_id_dcm = 1190273) then 'Y'

--win sf 2018
          when @campaign_id = 21606196 AND (@site_id_dcm = 4926865 OR @site_id_dcm = 1669510 OR @site_id_dcm = 4167910) then 'Y'

--win ny 2018
          when @campaign_id = 21616302 AND (@site_id_dcm = 1995643 OR @site_id_dcm = 4928980) then 'Y'

              ----2018 olympics

               when @campaign_id = 20681311 then 'Y'
               when @campaign_id = 20609762 AND @site_id_dcm = 2937979 then 'N' --Flipboard
               when @campaign_id = 20609762 AND (@site_id_dcm = 1654929 OR @site_id_dcm = 3379026 OR @site_id_dcm = 4442708 OR @site_id_dcm = 4440401 OR @site_id_dcm = 1669385) then 'Y'

            ---gm acq 2018

            when @campaign_id = 20606595 then 'Y'

             -------Correction to SF 2017

                 when @campaign_id = 20177168 and
                      (@site_id_dcm = 1516084 -- Spotify
                   or @site_id_dcm = 1995643) -- Verve
                      then 'M'

                 when @campaign_id = 20177168 and @site_id_dcm <> 1516084
                    then 'Y'

        -------Correction to Denver 2017---------------------
           when @campaign_id = 20323941
                 then 'Y'

               -- Corrections to SME 2016
               when @campaign_id = 10090315 and
                   (@site_id_dcm = 1513807 or   -- Inc
                    @site_id_dcm = 1592652)     -- Xaxis
                   then 'Y'

          -------Correction to Trade 2017, Vistar Media; MOAT tag not working, so using DV

         when @campaign_id = 11069476 and
                   (@site_id_dcm = 2244596) ---Vistar Media
                   then 'Y'

          ----- Corrections to SFO-SIN 2016
               when @campaign_id = 9923634 and
                  ((@site_id_dcm = 1534879 and @cost_method = 'CPM') or   -- Business Insider
                   (@site_id_dcm = 1853564))                                -- Live Intent
                   then 'N'


                   ----- Corrections to Trade additional partners 2018
                        when @campaign_id = 21415122
                            then 'Y'

          ----- Corrections to GM Acquisition, Prospecting Partners
               when @campaign_id = 10742878 and
                  ((@site_id_dcm = 1578478 and @placement like '%[Mm]obile_GOOGLE INC_GEN_INT_PROS_FT%') or   -- Google
                   (@site_id_dcm = 3267410 and @placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%') or  --Quantcast
                   (@site_id_dcm = 1239319 and @placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%') or  --Sojern
                   (@site_id_dcm = 1190273 and @placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%') or   --Adara
                   (@site_id_dcm = 1853562 and @placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%')  --Amobee
                  )
                   then 'Y'


               -- FlipBoard unable to implement Moat tags; must bill off of DFA impressions
               when @site_id_dcm = 2937979 then 'N'
               -- All targeted marketing subject to viewability; mark "Y"
               when @campaign_id = 9639387 then 'Y'

               -- If it's CPMV and the placement has these words, then it should be in Moat
               when @cost_method = 'CPMV' and
                   (@placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%' or
                    @placement like '%[Vv][Ii][Dd][Ee][Oo]%' or
                    @placement like '%[Pp][Rr][Ee]%[Rr][Oo][Ll][Ll]%' or
                    @site_id_dcm = 1995643  -- Verve
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
