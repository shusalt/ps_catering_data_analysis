# 不用了 eliminate(淘汰)
# drop table if exists data_analy_member_flavor_pre;
# create table if not exists data_analy_member_flavor_pre(
#     `member_id` bigint comment '会员id',
#     `member_name` varchar(10) comment '会员名',
#     `flavor` varchar(10) comment '口味',
#     `rn` int comment '排名'
# )
# duplicate key(`member_id`, `member_name`, `flavor`)
# distributed by hash(`member_id`) buckets 1
# properties(
#     "replication_num" = "1"
# );
#
#
#
#
# insert into data_analy_member_flavor_pre
# select
#     member_id,
#     member_name,
#     flavor,
#     rn
# from (
#     select
#         member_id,
#         member_name,
#         flavor,
#         ct,
#         row_number() over (partition by member_name order by ct desc) rn
#     from (
#         select
#             member_id,
#             member_name,
#             flavor,
#             sum(quantity) ct
#         from (
#             select
#                 order_detail.member_id,
#                 member_info.member_name,
#                 order_detail.dish_id,
#                 dish_info.dish_category,
#                 dish_info.flavor,
#                 quantity,
#                 total_sales
#             from (
#                 select
#                     member_id,
#                     dish_id,
#                     quantity,
#                     price * quantity total_sales
#                 from dwd_order_detail
#             ) order_detail
#             join (
#                 select
#                     dish_id,
#                     dish_category,
#                     flavor
#                 from dim_dish_info
#             ) dish_info
#             on order_detail.dish_id = dish_info.dish_id
#             join (
#                 select
#                     member_id,
#                     member_name
#                 from dim_member_info
#             ) member_info
#             on order_detail.member_id = member_info.member_id
#         ) tb1
#         group by member_id, member_name, flavor
#     ) tb2
# ) tb3
# where rn <= 3;
#
#
#
# select
#     member_id,
#     member_name,
#     dish_category,
#     ct
# from (
#     select
#         member_id,
#         member_name,
#         dish_category,
#         ct,
#         row_number() over (partition by member_name order by ct desc) rn
#     from (
#         select
#             member_id,
#             member_name,
#             dish_category,
#             sum(quantity) ct
#         from (
#             select
#                 order_detail.member_id,
#                 member_info.member_name,
#                 order_detail.dish_id,
#                 dish_info.dish_category,
#                 quantity,
#                 total_sales
#             from (
#                 select
#                     member_id,
#                     dish_id,
#                     quantity,
#                     price * quantity total_sales
#                 from dwd_order_detail
#             ) order_detail
#             join (
#                 select
#                     dish_id,
#                     dish_category
#                 from dim_dish_info
#             ) dish_info
#             on order_detail.dish_id = dish_info.dish_id
#             join (
#                 select
#                     member_id,
#                     member_name
#                 from dim_member_info
#             ) member_info
#             on order_detail.member_id = member_info.member_id
#         ) tb1
#         group by member_id, member_name, dish_category
#     ) tb2
# ) tb3
# where rn <= 3;


select
    flavor,
    count(flavor)
from dim_dish_info
group by flavor;





-- 酸：酸甜、酸、柠檬味
-- 甜：香甜、果味、柠檬味、甜、酸甜
-- 辣：微辣、香辣、麻辣、油腻、酸辣、辣、中辣
-- 鲜：咸鲜
-- 香：酱香、香草味、奶香、葱香、蒜蓉、蒜香、清香、香酥
-- 淡：清淡、爽口、原味

-- 创建一个flavor字典表
drop table if exists flavor_dic;
create table if not exists flavor_dic(
    `id` int comment 'id',
    `base_flavor` varchar(10) comment '基础口味',
    `composite_flavor` varchar(10) comment  '复合口味'
)
unique key(`id`)
distributed by hash(`id`) buckets 1
properties(
    "replication_num" = "1"
);



insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1002, '酸', '酸');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1003, '酸', '柠檬味');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1005, '甜', '甜');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1006, '甜', '香甜');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1007, '甜', '酸甜');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1008, '甜', '果味');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1009, '辣', '微辣');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1010, '辣', '香辣');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1011, '辣', '麻辣');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1012, '辣', '油腻');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1013, '辣', '酸辣');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1014, '辣', '辣');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1015, '辣', '中辣');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1016, '鲜', '咸鲜');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1017, '香', '酱香');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1018, '香', '香草味');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1019, '香', '奶香');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1020, '香', '葱香');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1021, '香', '蒜蓉');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1022, '香', '蒜香');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1023, '香', '清香');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1024, '香', '香酥');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1025, '淡', '清淡');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1026, '淡', '爽口');
insert into flavor_dic (`id`, `base_flavor`, `composite_flavor`) values (1027, '淡', '原味');



drop table if exists data_analysis_member_flavor_pre;
create table if not exists data_analysis_member_flavor_pre(
    `member_id` bigint comment '会员id',
    `member_name` varchar(10) comment '会员名',
    `base_flavor` varchar(10) comment '基础口味',
    `consumption_count` bigint comment '消防次数'
)
duplicate key(`member_id`, `member_name`, `base_flavor`)
distributed by hash(`member_id`) buckets 1
properties(
    "replication_num" = "1"
);




insert into data_analysis_member_flavor_pre
-- 会员对base_flavor的消费次数(会员的口味偏好)
select
    tb3.member_id,
    tb3.member_name,
    base_flavor_dic.base_flavor,
    sum(consumption_count) consumption_count
from (
    -- 查询出所有组合
    select
        member_flavor.member_id,
        member_flavor.member_name,
        member_flavor.composite_flavor,
        -- 没有消费记录的记录为0
        coalesce(order_info.ct, 0) consumption_count
    from (
        -- 会员与口味的所有组合
        select
            member_id,
            member_name,
            composite_flavor
        from (
            select
                member_id,
                member_name
            from dim_member_info
        ) member
        cross join (
            select
                composite_flavor
            from flavor_dic
        ) flavor
    ) member_flavor
    left join (
        -- 按照会员id、口味分组，统计消费次数
        select
            member_id,
            member_name,
            flavor,
            sum(quantity) ct
        from (
            select
                order_detail.member_id,
                member_info.member_name,
                order_detail.dish_id,
                dish_info.dish_category,
                dish_info.flavor,
                quantity,
                total_sales
            from (
                select
                    member_id,
                    dish_id,
                    quantity,
                    price * quantity total_sales
                from dwd_order_detail
            ) order_detail
            join (
                select
                    dish_id,
                    dish_category,
                    flavor
                from dim_dish_info
            ) dish_info
            on order_detail.dish_id = dish_info.dish_id
            join (
                select
                    member_id,
                    member_name
                from dim_member_info
            ) member_info
            on order_detail.member_id = member_info.member_id
        ) tb1
        group by member_id, member_name, flavor
    ) order_info
    on member_flavor.member_id = order_info.member_id
           and member_flavor.composite_flavor = order_info.flavor
) tb3
join (
    select
        composite_flavor,
        base_flavor
    from flavor_dic
) base_flavor_dic
on tb3.composite_flavor = base_flavor_dic.composite_flavor
group by tb3.member_id,
         tb3.member_name,
         base_flavor_dic.base_flavor;