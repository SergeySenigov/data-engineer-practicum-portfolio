
drop table if exists SENIGOVYANDEXRU__STAGING.group_log;

create table SENIGOVYANDEXRU__STAGING.group_log
(
    group_id   integer ,
    user_id   integer ,
    user_id_from integer ,
    event varchar(10) ,
    event_datetime timestamp
)
order by group_id, user_id
SEGMENTED BY hash(group_id) all nodes
PARTITION BY event_datetime::date
GROUP BY calendar_hierarchy_day(event_datetime::date, 3, 2) 
;

ALTER TABLE SENIGOVYANDEXRU__STAGING.group_log ADD CONSTRAINT fk_group_log_groups_group_id FOREIGN KEY (group_id) references SENIGOVYANDEXRU__STAGING.groups (id);
ALTER TABLE SENIGOVYANDEXRU__STAGING.group_log ADD CONSTRAINT fk_group_log_users_user_id_from FOREIGN KEY (user_id) references SENIGOVYANDEXRU__STAGING.users (id);
ALTER TABLE SENIGOVYANDEXRU__STAGING.group_log ADD CONSTRAINT fk_group_log_users_user_id FOREIGN KEY (user_id_from) references SENIGOVYANDEXRU__STAGING.users (id);


drop table if exists SENIGOVYANDEXRU__DWH.l_user_group_activity ;

create table SENIGOVYANDEXRU__DWH.l_user_group_activity
(
   hk_l_user_group_activity integer PRIMARY KEY,
   hk_user_id int not null,
   hk_group_id int not null,
   load_dt timestamp not null,
   load_src VARCHAR(20)
);

ALTER TABLE SENIGOVYANDEXRU__DWH.l_user_group_activity ADD CONSTRAINT fk_uga_users_hk_user_id FOREIGN KEY (hk_user_id) references SENIGOVYANDEXRU__DWH.h_users(hk_user_id);
ALTER TABLE SENIGOVYANDEXRU__DWH.l_user_group_activity ADD CONSTRAINT fk_uga_groups_hk_group_id FOREIGN KEY (hk_group_id) references SENIGOVYANDEXRU__DWH.h_groups(hk_group_id);


drop table if exists SENIGOVYANDEXRU__DWH.s_auth_history ;

create table SENIGOVYANDEXRU__DWH.s_auth_history
(
   hk_l_user_group_activity integer ,
   user_id_from int not null,
   event varchar(10) ,
   event_dt timestamp,
   load_dt timestamp not null,
   load_src VARCHAR(20)
);

ALTER TABLE SENIGOVYANDEXRU__DWH.s_auth_history ADD CONSTRAINT fk_auth_hist_uga FOREIGN KEY (hk_l_user_group_activity) references SENIGOVYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity);
