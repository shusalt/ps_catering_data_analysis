-- dim_member_info 会员维度表
drop table if exists dim_member_info;
create table if not exists dim_member_info(
    member_id bigint comment '会员号',
    member_name varchar(10) comment '会员名',
    gender varchar(3) comment '性别',
    age int comment '年龄',
    membership_join_date datetime comment '入会时间',
    phone_number bigint comment '手机号',
    membership_level varchar(10) comment '会员等级'
)
unique key(`member_id`)
distributed by hash(`member_id`) buckets 1
properties(
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "bloom_filter_columns"="member_name,phone_number"
);


-- 为dim_member_info创建bitmap的索引
create index if not exists membership_level_idx on dim_member_info (membership_level) using bitmap comment '会员等级字段索引';

-- dim_dish_info 菜品维度表
drop table if exists dim_dish_info;
create table if not exists dim_dish_info(
    dish_id bigint comment '菜品id',
    dish_name varchar(64) comment '菜品名称',
    flavor varchar(10) comment '菜品口味',
    price decimal(16, 2) comment '价格',
    cost decimal(16, 2) comment '成本',
    recommendation_level float comment '推荐度',
    dish_category varchar(32) comment '菜品类别'
)
unique key(`dish_id`)
distributed by hash(`dish_id`) buckets 1
properties(
    "replication_num" = "1",
    "anable_unique_key_merge_on_write" = "true",
    "bloom_filter_columns" = "dish_name"
);


-- 为dim_dish_info创建bitmap索引
create index if not exists dish_category_idx on dim_dish_info (dish_category) using bitmap comment '菜品类别列索引';
drop index if exists dish_category_idx on dim_dish_info;

-- ods to dim_member_info
insert into private_station.dim_member_info
select
    member_id,
    member_name,
    gender,
    age,
    member_join_date,
    phone_number,
    membership_level
from private_station.ods_member_info;



-- ods to dim_dish_info
insert into private_station.dim_dish_info
select
    dish_id,
    dish_name,
    flavor,
    price,
    cost,
    recommendation_level,
    -- '[[:space:]]+$' 去除字符串末尾任何空格字符
    regexp_replace(dish_category, '[[:space:]]+$', '')
from private_station.ods_dish_info;


-- dwd_order_detail 下单明细事实表
drop table if exists dwd_order_detail;
create table if not exists dwd_order_detail(
    `order_id` bigint comment '订单id',
    `member_id` bigint comment '会员id',
    `dish_id` bigint comment '菜品id',
    `shop_name` varchar(32) comment '店铺名',
    `shop_location` varchar(10) comment '店铺所在地',
    `order_time` datetime comment '点餐时间',
    `payment_time` datetime comment '结算时间',
    `is_paid` int comment '是否结算(0:未结算,1:已结算)',
    `consumption_amount` decimal(16, 2) comment '消费金额',
    `price` decimal(16, 2) comment '单品价格',
    `quantity` int comment '数量'
)
duplicate key(`order_id`, `member_id`, `dish_id`)
distributed by hash(`order_id`) buckets 1
properties(
    "replication_num" = "1",
    "bloom_filter_columns" = "member_id,dish_id"
);

-- 为dwd_order_detail创建bitmap索引
create index if not exists shop_name_idx on dwd_order_detail (shop_name) using bitmap comment '店铺名列索引';
create index if not exists shop_location_idx on dwd_order_detail (shop_location) using bitmap comment '店铺所在地列索引';


-- ods to dwd_order_detail
-- 首日装载
insert into private_station.dwd_order_detail
select
    order_info.order_id,
    member_info.member_id,
    dish_info.dish_id,
    order_info.shop_name,
    order_info.shop_location,
    order_info.order_time,
    order_info.payment_time,
    order_info.is_paid,
    order_info.consumption_amount,
    order_detail.price,
    order_detail.quantity
from (
    select
        if(substr(order_id, 9, 1) = '0',
           concat(substring(order_id, 1, 8), substring(order_id, 10)),
           order_id) order_id,
        member_name,
        shop_name,
        shop_location,
        order_time,
        consumption_amount,
        is_paid,
        payment_time
    from private_station.ods_order_info
) order_info
inner join(
    select
        order_id,
        dish_name,
        price,
        quantity
    from private_station.ods_order_detail
) order_detail
on order_info.order_id = order_detail.order_id
inner join(
    select
        member_id,
        member_name
    from private_station.ods_member_info
) member_info
on order_info.member_name = member_info.member_name
inner join(
    select
        dish_id,
        dish_name
    from private_station.ods_dish_info
) dish_info
on order_detail.dish_name = dish_info.dish_name;


-- 每日装载
# insert into private_station.dwd_order_detail
select
    order_info.order_id,
    member_info.member_id,
    dish_info.dish_id,
    order_info.shop_name,
    order_info.shop_location,
    order_info.order_time,
    order_info.payment_time,
    order_info.is_paid,
    order_info.consumption_amount,
    order_detail.price,
    order_detail.quantity
from (
    select
        if(substr(order_id, 9, 1) = '0',
           concat(substring(order_id, 1, 8), substring(order_id, 10)),
           order_id) order_id,
        member_name,
        shop_name,
        shop_location,
        order_time,
        consumption_amount,
        is_paid,
        payment_time
    from private_station.ods_order_info
    -- 索取当日时间
    where date(payment_time) = date_add('2016-09-01', -1)
) order_info
inner join(
    select
        order_id,
        dish_name,
        price,
        quantity
    from private_station.ods_order_detail
) order_detail
on order_info.order_id = order_detail.order_id
inner join(
    select
        member_id,
        member_name
    from private_station.ods_member_info
) member_info
on order_info.member_name = member_info.member_name
inner join(
    select
        dish_id,
        dish_name
    from private_station.ods_dish_info
) dish_info
on order_detail.dish_name = dish_info.dish_name;



-- 统计指标需求分析
select
    order_id,
    order_detail.member_id,
    order_detail.dish_id,
    shop_name,
    shop_location,
    order_time,
    payment_time,
    is_paid,
    consumption_amount,
    order_detail.price,
    quantity,
    dish_name,
    flavor,
    cost,
    recommendation_level,
    dish_category,
    member_name,
    gender,
    age,
    membership_join_date,
    phone_number,
    membership_level
from (
    select
        order_id,
        member_id,
        dish_id,
        shop_name,
        shop_location,
        order_time,
        payment_time,
        is_paid,
        consumption_amount,
        price,
        quantity
    from dwd_order_detail
) order_detail
left join (
    select
        dish_id,
        dish_name,
        flavor,
        price,
        cost,
        recommendation_level,
        dish_category
    from dim_dish_info
) dish_info
on order_detail.dish_id = dish_info.dish_id
left join (
    select
        member_id,
        member_name,
        gender,
        age,
        membership_join_date,
        phone_number,
        membership_level
    from dim_member_info
) member_info
on order_detail.member_id = member_info.member_id;