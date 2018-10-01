select distinct substring(other_data,(instr(other_data,'u2=') + 3),2)                       as "POS"
from diap01.mec_us_united_20056.dfa2_activity
where cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date ) between '2018-01-01' and '2018-06-22'
and other_data like '%u2=%'
