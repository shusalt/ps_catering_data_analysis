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


-- 会员口味偏好统计
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


-- 会员口味偏好聚类分析结果
drop table if exists da_member_flavor_predictions;
create table if not exists da_member_flavor_predictions(
    `member_id` bigint comment '会员id',
    `member_name` varchar(10) comment '会员名',
    `light_taste` int comment '淡口味的消费次数',
    `sweet_taste` int comment '甜口味的消费次数',
    `spicy_taste` int comment '辣口味的消费次数',
    `sour_taste` int comment '酸口味的消费次数',
    `fragrant_taste` int comment '香口味的消费次数',
    `fresh_taste` int comment  '鲜口味的消费次数',
    `cluster_id` int comment '会员群号'
)
duplicate key (`member_id`)
distributed by hash(`member_id`) buckets 1
properties(
    "replication_num" = "1"
);


-- 会员口味偏好聚类分析会员簇结果
drop table if exists mfp_cluster_analysis_result;
create table if not exists mfp_cluster_analysis_result(
    `feature_name` varchar(10) comment '口味特征',
    `member_cluster0` double comment '会员群0',
    `member_cluster1` double comment '会员群1',
    `member_cluster2` double comment '会员群2',
    `member_cluster3` double comment '会员群3',
    `member_cluster4` double comment '会员群4',
    `member_cluster5` double comment '会员群5'
)
duplicate key(`feature_name`)
distributed by hash(`feature_name`) buckets 1
properties(
    "replication_num" = "1"
);


select
    dish_category,
    count(1)
from dim_dish_info
group by dish_category;


-- dish_category_dic 品类字典
drop table if exists dish_category_dic;
create table if not exists dish_category_dic(
    `id` int comment 'id',
    `base_category` varchar(32) comment '基础品类',
    `composite_category` varchar(32) comment '细分品类'
)
unique key(`id`)
distributed by hash(`id`) buckets 1
properties (
    "replication_num" = "1"
);



-- 肉类：猪肉类、羊肉类、其他肉类、牛肉类、家禽类
-- 海_河鲜类：鱼类、蟹类、虾类、其他水产、贝壳类
-- 饮料类：饮料类
-- 酒类：红酒类、啤酒类、白酒类
-- 果蔬类：叶菜类、茎菜类、花菜类、海藻类、果菜类、根菜类
-- 小吃类：粥类、甜点类、糕点类、肠粉类、面包类
-- 主食类：米饭类、面食类
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1001, '肉类', '猪肉类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1002, '肉类', '羊肉类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1003, '肉类', '其他肉类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1004, '肉类', '牛肉类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1005, '肉类', '家禽类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1006, '海_河鲜类', '鱼类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1007, '海_河鲜类', '蟹类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1008, '海_河鲜类', '虾类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1009, '海_河鲜类', '其他水产');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1010, '海_河鲜类', '贝壳类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1011, '饮料类', '饮料类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1012, '酒类', '红酒类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1013, '酒类', '啤酒类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1014, '酒类', '白酒类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1015, '果蔬类', '叶菜类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1016, '果蔬类', '茎菜类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1017, '果蔬类', '花菜类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1018, '果蔬类', '海藻类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1019, '果蔬类', '果菜类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1020, '果蔬类', '根菜类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1021, '小吃类', '粥类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1022, '小吃类', '甜点类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1023, '小吃类', '糕点类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1024, '小吃类', '肠粉类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1025, '小吃类', '面包类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1026, '主食类', '米饭类');
insert into dish_category_dic (`id`, `base_category`, `composite_category`) VALUES (1027, '主食类', '面食类');




-- 会员品类偏好统计
drop table if exists da_member_category_pre;
create table if not exists da_member_category_pre(
    `member_id` bigint comment '会员id',
    `member_name` varchar(10) comment '会员名',
    `base_category` varchar(32) comment '基础品类',
    `composite_category` int comment '消费次数'
)
duplicate key(`member_id`)
distributed by hash(`member_id`) buckets 1
properties(
    "replication_num" = "1"
);


insert into da_member_category_pre
select
    tb3.member_id,
    tb3.member_name,
    category_dic.base_category,
    sum(tb3.consumption_count) composite_category
from (
    select
        member_category.member_id,
        member_category.member_name,
        member_category.category_id,
        member_category.composite_category,
        coalesce(tb2.ct, 0) consumption_count
    from (
        -- 会员与品类的所有组合
        select
            member_id,
            member_name,
            category_id,
            composite_category
        from (
            select
                member_id,
                member_name
            from dim_member_info
        ) member_info
        cross join (
            select
                id category_id,
                composite_category
            from dish_category_dic
        ) category_dic
    ) member_category
    left join (
        -- 会员的品类消费次数
        select
            member_id,
            member_name,
            dish_category,
            sum(quantity) ct
        from (
            select
                order_detail.member_id,
                member.member_name,
                dish.dish_category,
                order_detail.quantity
            from (
                select
                    member_id,
                    dish_id,
                    quantity
                from dwd_order_detail
            ) order_detail
            inner join (
                select
                    member_id,
                    member_name
                from dim_member_info
            ) member
            on order_detail.member_id = member.member_id
            inner join (
                select
                    dish_id,
                    dish_category
                from dim_dish_info
            ) dish
            on order_detail.dish_id = dish.dish_id
        ) tb1
        group by member_id,
            member_name,
            dish_category
    ) tb2
    on member_category.member_id = tb2.member_id and
        member_category.composite_category = tb2.dish_category
) tb3
join (
    select
        id,
        base_category
    from dish_category_dic
) category_dic
on tb3.category_id = category_dic.id
group by member_id,
    member_name,
    base_category;




-- 会员对品类偏好聚类分析结果
drop table if exists da_member_category_predictions;
create table if not exists da_member_category_predictions(
    `member_id` bigint comment '会员id',
    `member_name` varchar(10) comment '会员名称',
    `staple_foods` int comment '主食类消费次数',
    `snacks` int comment '小吃类消费次数',
    `fruits_and_vegetables` int comment '果蔬类消费次数',
    `seafood_or_river_delicacies` int comment '海/河鲜类消费次数',
    `meat` int comment '肉食类消费次数',
    `liquor` int comment '酒类消费次数',
    `beverages` int comment '饮料类消费次数',
    `prediction` int comment '聚类结果'
)
duplicate key(`member_id`)
distributed by hash(`member_id`) buckets 1
properties(
    "replication_num" = "1"
);



-- 创建聚类分析聚类中心结果
drop table if exists mc_cluster_center_analysis_result;
create table if not exists mc_cluster_center_analysis_result(
    `features` varchar(20) comment '品类特征',
    `member_cluster0` double comment '会员群0中心',
    `member_cluster1` double comment '会员群1中心',
    `member_cluster2` double comment '会员群2中心',
    `member_cluster3` double comment '会员群3中心'
)
duplicate key(`features`)
distributed by hash(`features`) buckets 1
properties(
    "replication_num" = "1"
);



-- 菜品相关性分析
select
    dish_name
from dim_dish_info
group by dish_name;


#     select
#         member_id,
#         member_name,
#         dish_name,
#         total_sales,
#         count(dish_name) over(partition by member_id) sum_num
#     from (
#         select
#             member_id,
#             member_name,
#             dish_name,
#             sum(sales) total_sales
#         from (
#             select
#                 order_detail.member_id,
#                 member.member_name,
#                 dish.dish_name,
#                 order_detail.sales
#             from (
#                 select
#                     member_id,
#                     dish_id,
#                     quantity * price as sales
#                 from dwd_order_detail
#             ) order_detail
#             inner join (
#                 select
#                     member_id,
#                     member_name
#                 from dim_member_info
#             ) member
#             on order_detail.member_id = member.member_id
#             inner join (
#                 select
#                     dish_id,
#                     dish_name
#                 from dim_dish_info
#             ) dish
#             on order_detail.dish_id = dish.dish_id
#         ) tb1
#         group by member_id, member_name, dish_name
#     ) tb2

-- 时间序列销量数据
drop table if exists da_date_series_amount_analysis;
create table if not exists da_date_series_amount_analysis(
    `date` varchar(32) comment '日期',
    `quantity` int comment '销量',
    `total_sales` float comment '总额'
)
duplicate key(`date`)
distributed by hash(`date`) buckets 1
properties(
    "replication_num" = "1"
);



insert into da_date_series_amount_analysis
select
    date_format(payment_time, '%Y-%m-%d') date,
    sum(quantity) quantity,
    sum(quantity * price) total_sales
from dwd_order_detail
where payment_time is not null
group by date_format(payment_time, '%Y-%m-%d')
order by date;