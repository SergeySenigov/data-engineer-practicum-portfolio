with user_group_log as (
    select hg.hk_group_id, hg.registration_dt, count(DISTINCT luga.hk_user_id) cnt_added_users
    from SENIGOVYANDEXRU__DWH.h_groups hg 
    left join SENIGOVYANDEXRU__DWH.l_user_group_activity luga on luga.hk_group_id = hg.hk_group_id
    left join SENIGOVYANDEXRU__DWH.s_auth_history sah on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity 
    where sah.event = 'add'
    group by hg.hk_group_id, hg.registration_dt
    order by hg.registration_dt 
    limit 10
)
select hk_group_id, cnt_added_users
from user_group_log
order by cnt_added_users
limit 10 	;