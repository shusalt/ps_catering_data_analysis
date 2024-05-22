-- 1日各会员各类别的stat
drop table if exists dws_member_dish_stat;
create table if not exists dws_member_dish_stat(
    member_id bigint comment '用户id',
    member_name varchar(10) comment '用户名',
    dish_category varchar(32) comment '类品名称',
    pay_date date comment '支付日期',
    order_count int sum default '0' comment '订单量',
    slaves_volume int sum default '0' comment '销量',
    total_sales decimal(16, 2)  sum default '0' comment '销售额'
)
aggregate key(`member_id`, `member_name`, `dish_category`, `pay_date`)
distributed by hash(`member_id`) buckets 1
properties(
    "replication_num" = "1"
);

-- 为 dws_member_dish_stat 的 dish_id,dish_category创建bitmap索引
create index if not exists dish_name_idx on dws_member_dish_stat (dish_category) using bitmap comment '类品名称列bitmap索引';

-- 首日装载
insert into private_station.dws_member_dish_stat
select
    order_detail.member_id,
    member_info.member_name,
    dish_info.dish_category,
    date(payment_time) pay_date,
    bitmap_count(bitmap_union(to_bitmap(order_id))) order_count,
    sum(order_detail.quantity) slaves_volume,
    sum(order_detail.quantity * order_detail.price) total_sales
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
    where is_paid = '1'
) order_detail
left join (
    select
        member_id,
        member_name
    from dim_member_info
) member_info
on order_detail.member_id = member_info.member_id
left join (
    select
        dish_id,
        dish_category
    from dim_dish_info
) dish_info
on order_detail.dish_id = dish_info.dish_id
group by
    order_detail.member_id,
    member_info.member_name,
    dish_info.dish_category,
    date(payment_time);

-- 每日装载
# insert into private_station.dws_member_dish_stat
select
    order_detail.member_id,
    member_info.member_name,
    dish_info.dish_category,
    date(payment_time) pay_date,
    bitmap_count(bitmap_union(to_bitmap(order_id))) order_count,
    sum(order_detail.quantity) slaves_volume,
    sum(order_detail.quantity * order_detail.price) total_sales
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
    -- 获取当日增量数据
    where is_paid = '1' and date(payment_time) = date_add('2016-09-01', -1)
) order_detail
left join (
    select
        member_id,
        member_name
    from dim_member_info
) member_info
on order_detail.member_id = member_info.member_id
left join (
    select
        dish_id,
        dish_category
    from dim_dish_info
) dish_info
on order_detail.dish_id = dish_info.dish_id
group by
    order_detail.member_id,
    member_info.member_name,
    dish_info.dish_category,
    date(payment_time);


-- 1日各城市各店铺的stat
drop table if exists dws_shop_city_stat;
create table if not exists dws_shop_city_stat(
    `shop_location` varchar(10) comment '店铺所在地',
    `shop_name` varchar(32) comment '店铺名',
    `pay_date` date comment '支付日期',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`shop_location`, `shop_name`, `pay_date`)
distributed by hash(`shop_location`) buckets 1
properties(
    "replication_num" = "1"
);

-- 为shop_location，shop_name创建bitmap索引
create index if not exists shop_location_idx on dws_shop_city_stat (shop_location) using bitmap comment 'shop_location列bitmap索引';

create index if not exists shop_name_idx on dws_shop_city_stat (shop_name) using bitmap comment 'shop_name列bitmap索引';


-- 数据装载
-- 首日装载
insert into dws_shop_city_stat
select
    shop_location,
    shop_name,
    date(payment_time),
    bitmap_count(bitmap_union(to_bitmap(order_id))) order_count,
    sum(quantity),
    sum(quantity * price)
from dwd_order_detail
where is_paid = '1'
group by
    shop_location,
    shop_name,
    date(payment_time);

-- 每日装载
# insert into dws_shop_city_stat
select
    shop_location,
    shop_name,
    date(payment_time),
    bitmap_count(bitmap_union(to_bitmap(order_id))) order_count,
    sum(quantity),
    sum(quantity * price)
from dwd_order_detail
-- 获取当日增量数据
where is_paid = '1' and date(payment_time) = date_add('2016-09-01', -1)
group by
    shop_location,
    shop_name,
    date(payment_time);


-- 1日各品类各口味各菜品的stat
drop table if exists dws_dish_stat;
create table if not exists dws_dish_stat(
    `dish_id` bigint comment '菜品id',
    `dish_name` varchar(64) comment '菜品名称',
    `flavor` varchar(10) comment '菜品口味',
    `dish_category` varchar(32) comment '菜品类别',
    `pay_date` date comment '支付日期',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销量',
    `total_sales` decimal(16, 2) sum default  '0' comment '销售额'
)
aggregate key(`dish_id`, `dish_name`, `flavor`, `dish_category`, `pay_date`)
distributed by hash(`dish_id`) buckets 1
properties(
    "replication_num" = "1",
    "bloom_filter_columns" = "dish_id, dish_name, pay_date"
);


create index if not exists flavor_idx on dws_dish_stat (flavor) using bitmap comment 'flavor列bitmap索引';
create index if not exists dish_category_idx on dws_dish_stat (dish_category) using bitmap comment 'dish_categoru列索引';


-- 首日装载
insert into dws_dish_stat
select
    order_detail.dish_id,
    dish_info.dish_name,
    dish_info.flavor,
    dish_info.dish_category,
    date(order_detail.payment_time),
    bitmap_count(bitmap_union(to_bitmap(order_detail.order_id))),
    sum(order_detail.quantity),
    sum(order_detail.quantity*order_detail.price)
from (
    select
        order_id,
        payment_time,
        dish_id,
        quantity,
        price
    from dwd_order_detail
    where is_paid = '1'
) order_detail
inner join (
    select
        dish_id,
        dish_category,
        flavor,
        dish_name
    from dim_dish_info
) dish_info
on order_detail.dish_id = dish_info.dish_id
group by
    order_detail.dish_id,
    dish_info.dish_name,
    dish_info.flavor,
    dish_info.dish_category,
    date(order_detail.payment_time);


-- 每日装载
# insert into dws_dish_stat
select
    order_detail.dish_id,
    dish_info.dish_name,
    dish_info.flavor,
    dish_info.dish_category,
    date(order_detail.payment_time),
    bitmap_count(bitmap_union(to_bitmap(order_detail.order_id))),
    sum(order_detail.quantity),
    sum(order_detail.quantity*order_detail.price)
from (
    select
        order_id,
        payment_time,
        dish_id,
        quantity,
        price
    from dwd_order_detail
    -- 获取当日增量数据
    where is_paid = '1' and date(payment_time) = date_add('2016-09-01', -1)
) order_detail
inner join (
    select
        dish_id,
        dish_category,
        flavor,
        dish_name
    from dim_dish_info
) dish_info
on order_detail.dish_id = dish_info.dish_id
group by
    order_detail.dish_id,
    dish_info.dish_name,
    dish_info.flavor,
    dish_info.dish_category,
    date(order_detail.payment_time);