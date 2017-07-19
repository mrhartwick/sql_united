case when state in ('il','in','mi','oh','wi','ia','ks','mn','mo','ne','nd','sd') then 'midwest'
     when state in ('nj','ny','pa','ct','me','ma','nh','ri''vt') then 'northeast'
     when state in ('al','ky','ms','tn','de','fl','ga','md','nc','sc','va','wv','ar','la','ok','tx') then 'south'
     when state in ('az','co','id','mt','nv','nm','ut','wy','ca','or','wa') then 'west'
     when state in ('ak','hi') then 'ak_hi'
else 'xxx' end as region



