select
  imps,
  sum(imps)      as imps,
  sum(clicks)    as clicks,
  count(user_id) as user_id,
  sum(dbm_cost)  as dbm_cost,
  sum(con)       as con,
  sum(tix)       as tix,
  sum(rev)       as rev,
  sum(vew_con)   as vew_con,
  sum(vew_tix)   as vew_tix,
  sum(vew_rev)   as vew_rev,
  sum(clk_con)   as clk_con,
  sum(clk_tix)   as clk_tix,
  sum(clk_rev)   as clk_rev
from diap01.mec_us_united_20056.ual_freq_dbm_cost
group by imps