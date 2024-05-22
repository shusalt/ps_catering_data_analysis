-- ads_dish_catagory_1day, 每日各品类别的销售指标
drop table if exists ads_dish_category_1day;
create table if not exists ads_dish_category_1day(
    `pay_date` date comment '支付日期',
    `dish_category` varchar(64) comment '品类',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`pay_date`, `dish_category`)
distributed by hash(`pay_date`) buckets 1
properties(
    "replication_num" = "1"
);

create index dish_category_idx on ads_dish_category_1day (dish_category) using bitmap comment 'dish_category列bitmap索引';

insert into ads_dish_category_1day
select
    pay_date,
    dish_category,
    sum(order_count) order_count,
    sum(slaves_volume) slaves_volume,
    sum(total_sales) total_sales
from dws_member_dish_stat
group by pay_date, dish_category
order by pay_date, dish_category;


-- ads_dish_category 历史累积品类指标
drop table if exists ads_dish_category;
create table if not exists ads_dish_categroy(
    `dish_category` varchar(64) comment '品类',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`dish_category`)
distributed by hash(`dish_category`) buckets 1
properties(
    "replication_num" = "1"
);

create index dish_category_idx on ads_dish_categroy (dish_category) using bitmap comment 'dish_category列bitmap索引';

insert into ads_dish_categroy
select
    dish_category,
    order_count,
    slaves_volume,
    total_sales
from ads_dish_category_1day;


-- ads_city_1day 每日城市的统计指标
drop table if exists ads_city_1day;
create table if not exists ads_city_1day(
    `pay_date` date comment '支付日期',
    `shop_location` varchar(10) comment '店铺所在地',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销量',
    `total_sales` decimal sum default '0' comment '销售额'
)
aggregate key(`pay_date`, `shop_location`)
distributed by hash(`pay_date`) buckets 1
properties(
    "replication_num" = "1",
    "bloom_filter_columns" = "pay_date"
);

create index shop_location_idx on ads_city_1day (shop_location) using bitmap comment 'shop_location列bitmap索引';

insert into ads_city_1day
select
    pay_date,
    shop_location,
    order_count,
    slaves_volume,
    total_sales
from dws_shop_city_stat;


-- ads_city 历史累积城市的统计指标
drop table if exists ads_city;
create table if not exists ads_city(
    `shop_location` varchar(10) comment '店铺所在地',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销量',
    `total_sales` decimal sum default '0' comment '销售额'
)
aggregate key(`shop_location`)
distributed by hash(`shop_location`) buckets 1
properties(
    "replication_num" = "1"
);

create index shop_location_idx on ads_city (shop_location) using bitmap comment 'shop_location列bitmap索引';

insert into ads_city
select
    shop_location,
    order_count,
    slaves_volume,
    total_sales
from ads_city_1day;


-- ads_shop_1day 每日店铺的统计指标
drop table if exists ads_shop_1day;
create table if not exists ads_shop_1day(
    `pay_date` date comment '支付时间',
    `shop_name` varchar(32) comment '店铺名',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`pay_date`, `shop_name`)
distributed by hash(`pay_date`) buckets 1
properties(
    "replication_num" = "1",
    "bloom_filter_columns" = "pay_date"
);

create index if not exists shop_name_idx on ads_shop_1day (shop_name) using bitmap comment 'shop_name列bitmap索引';

insert into ads_shop_1day
select
    pay_date,
    shop_name,
    order_count,
    slaves_volume,
    total_sales
from dws_shop_city_stat;

-- ads_shop 历史累积店铺的统计指标
drop table if exists ads_shop;
create table if not exists ads_shop(
    `shop_name` varchar(32) comment '店铺名',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`shop_name`)
distributed by hash(`shop_name`) buckets 1
properties(
    "replication_num" = "1"
);

create index if not exists shop_name_idx on ads_shop (shop_name) using bitmap comment 'shop_name列bitmap索引';

insert into ads_shop
select
    shop_name,
    order_count,
    slaves_volume,
    total_sales
from ads_shop_1day;