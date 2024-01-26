delete from analysis.dm_rfm_segments;

insert into analysis.dm_rfm_segments(user_id, recency, frequency, monetary_value)

(select user_id, max(recency), max(frequency), max(monetary_value) from (
select t.user_id, t.recency, 0::int2 as frequency, 0::int2 as monetary_value
from analysis.tmp_rfm_recency  t
union all
select trf.user_id, 0, trf.frequency, 0
from analysis.tmp_rfm_frequency trf
union all
select trmv.user_id, 0, 0, trmv.monetary_value
from analysis.tmp_rfm_monetary_value trmv
) t
group by user_id);
