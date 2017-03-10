select
  imps,
  count(user_id) as user_id,
  sum(dbm_cost) as dbm_cost,
  sum(rev) as rev
  from diap01.mec_us_united_20056.ual_freq_dbm_cost
group by imps