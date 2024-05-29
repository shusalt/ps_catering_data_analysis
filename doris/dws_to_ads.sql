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

-- 首日装载
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

-- 每日装载
# insert into ads_dish_category_1day
select
    pay_date,
    dish_category,
    order_count,
    slaves_volume,
    total_sales
from dws_member_dish_stat
where pay_date = date_add('2016-09-01', -1);


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

-- 首日装载
insert into ads_dish_categroy
select
    dish_category,
    order_count,
    slaves_volume,
    total_sales
from ads_dish_category_1day;

-- 每日装载
# insert into ads_dish_categroy
select
    dish_category,
    order_count,
    slaves_volume,
    total_sales
from ads_dish_category_1day
where pay_date = date_add('2016-09-01', -1);



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

-- 首日装载
insert into ads_city_1day
select
    pay_date,
    shop_location,
    order_count,
    slaves_volume,
    total_sales
from dws_shop_city_stat;

-- 每日转载
# insert into ads_city_1day
select
    pay_date,
    shop_location,
    order_count,
    slaves_volume,
    total_sales
from dws_shop_city_stat
where pay_date = date_add('2016-09-01', -1);


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

-- 首日装载
insert into ads_city
select
    shop_location,
    order_count,
    slaves_volume,
    total_sales
from ads_city_1day;

-- 每日装载
# insert into ads_city
select
    shop_location,
    order_count,
    slaves_volume,
    total_sales
from ads_city_1day
where pay_date = date_add('2016-09-01', -1);


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

-- 首日装载
insert into ads_shop_1day
select
    pay_date,
    shop_name,
    order_count,
    slaves_volume,
    total_sales
from dws_shop_city_stat;

-- 每日装载
# insert into ads_shop_1day
select
    pay_date,
    shop_name,
    order_count,
    slaves_volume,
    total_sales
from dws_shop_city_stat
where pay_date = date_add('2016-09-01', -1);

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

-- 首日装载
insert into ads_shop
select
    shop_name,
    order_count,
    slaves_volume,
    total_sales
from ads_shop_1day;

-- 每日装载
# insert into ads_shop
select
    shop_name,
    order_count,
    slaves_volume,
    total_sales
from ads_shop_1day
where pay_date = date_add('2016-09-01', -1);

-- 月季各城市盈利额与各城市盈利总额平均值对比
drop table if exists ads_city_month;
create table if not exists ads_city_month(
    `month_date` varchar(10) comment 'yyyy-MM',
    `shop_location` varchar(10) comment '店铺所在地',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额',
    `score` decimal(16, 2) sum default '0' comment 'kpi评分',
    `target` decimal(16, 2) replace default '58000' comment '目标值'
)
aggregate key(`month_date`, `shop_location`)
distributed by hash(`month_date`) buckets 1
properties(
    "replication_num" = "1"
);

-- 首日装载
insert into ads_city_month
select
    date_format(pay_date, '%Y-%m'),
    shop_location,
    sum(order_count),
    sum(slaves_volume),
    sum(total_sales),
    sum(order_count) * 0.3 + sum(slaves_volume) * 0.2 + sum(total_sales) * 0.5 score,
    58000
from ads_city_1day
group by date_format(pay_date, '%Y-%m'),shop_location;


-- 每日装载
# insert into ads_city_month
select
    date_format(pay_date, '%Y-%m'),
    shop_location,
    order_count,
    slaves_volume,
    total_sales,
    order_count * 0.3 + slaves_volume * 0.2 + total_sales * 0.5 score,
    58000
from ads_city_1day
-- 每日新增的数据
where pay_date = date_add('2016-09-01', -1);



-- ads_shop_month 月季度各店铺盈利额与各店铺盈利总额平均值对比
drop table if exists ads_shop_month;
create table if not exists ads_shop_month(
    `month_date` varchar(10) comment 'yyyy-MM',
    `shop_name` varchar(32) comment '店铺名',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额',
    `score` decimal(16, 2) sum default '0' comment 'kpi评分',
    `target`  decimal(16, 2) replace default '0' comment '目标值'
)
aggregate key(`month_date`, `shop_name`)
distributed by hash(`month_date`) buckets 1
properties(
    "replication_num" = "1"
);


-- 首日装载
insert into ads_shop_month
select
    date_format(pay_date, '%Y-%m') date_month,
    shop_name,
    sum(order_count),
    sum(slaves_volume),
    sum(total_sales),
    sum(order_count) * 0.3 + sum(slaves_volume) * 0.2 + sum(total_sales) * 0.5 score,
    25798
from dws_shop_city_stat
group by date_month, shop_name;


-- 每日转载
# insert into ads_shop_month
select
    date_format(pay_date, '%Y-%m') date_month,
    shop_name,
    order_count,
    slaves_volume,
    total_sales,
    order_count * 0.3 + slaves_volume * 0.2 + total_sales * 0.5 score,
    25798
from dws_shop_city_stat
where pay_date = date_add('2016-09-01', -1);


-- 历史各会消费的统计指标
drop table if exists ads_member;
create table if not exists ads_member(
    `member_id` bigint comment '会员id',
    `member_name` varchar(10) comment '会员名',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销售量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`member_id`, `member_name`)
distributed by key(`member_id`) buckets 1
properties(
    "replication_num" = "1",
    "bloom_filter_columns" = "member_id, member_name"
);


-- 首日装载
insert into ads_member
select
    member_id,
    member_name,
    sum(order_count),
    sum(slaves_volume),
    sum(total_sales)
from dws_member_dish_stat
group by member_id, member_name;


-- 每日装载
# insert into ads_member
select
    member_id,
    member_name,
    order_count,
    slaves_volume,
    total_sales
from dws_member_dish_stat
where pay_date = date_add('2016-09-01', -1);


-- 历史至今各会员对品类消费情况
drop table if exists ads_member_dish_category;
create table if not exists ads_member_dish_category(
    `member_id` bigint comment '会员id',
    `member_name` varchar(10) comment '会员名',
    `dish_category` varchar(32) comment '品类',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销售量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`member_id`, `member_name`, `dish_category`)
distributed by hash(`member_id`) buckets 1
properties(
    "replication_num" = "1",
    "bloom_filter_columns" = "member_id, member_name"
);


-- 首日装载
insert into ads_member_dish_category
select
    member_id,
    member_name,
    dish_category,
    sum(order_count),
    sum(slaves_volume),
    sum(total_sales)
from dws_member_dish_stat
group by member_id, member_name, dish_category;



-- 每日数据装载
# insert into ads_member_dish_category
select
    member_id,
    member_name,
    dish_category,
    order_count,
    slaves_volume,
    total_sales
from dws_member_dish_stat
where pay_date = date_add('2016-09-01', -1);



-- 每日各菜品销售情况的统计指标
drop table if exists ads_dish_name_1day;
create table if not exists ads_dish_name_1day(
    `pay_date` date comment '支付日期',
    `dish_id` bigint comment '菜品id',
    `dish_name` varchar(64) comment '菜品名称',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销售量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`pay_date`, `dish_id`, `dish_name`)
distributed by hash(`pay_date`) buckets 1
properties(
    "replication_num" = "1"
);


-- 首日装载
insert into ads_dish_name_1day
select
    pay_date,
    dish_id,
    dish_name,
    sum(order_count),
    sum(slaves_volume),
    sum(total_sales)
from dws_dish_stat
group by pay_date, dish_id, dish_name;



-- 每日装载
# insert into ads_member_dish_category_1day
select
    pay_date,
    dish_id,
    dish_name,
    order_count,
    slaves_volume,
    total_sales
from dws_dish_stat
where pay_date = date_add('2016-09-01', -1);




-- 每月各菜品销售情况的统计指标
drop table if exists ads_dish_name_month;
create table if not exists ads_dish_name_month(
    `month_date` varchar(10) comment 'yyyy-MM',
    `dish_id` bigint comment '菜品id',
    `dish_name` varchar(64) comment '菜品名称',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销售量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`month_date`, `dish_id`, `dish_name`)
distributed by hash(`month_date`) buckets 1
properties(
    "replication_num" = "1"
);


-- 首日装载
insert into ads_dish_name_month
select
    date_format(pay_date, '%Y-%m') month_date,
    dish_id,
    dish_name,
    sum(order_count),
    sum(slaves_volume),
    sum(total_sales)
from ads_dish_name_1day
group by month_date, dish_id, dish_name;


-- 每日装载
# insert into ads_dish_name_month
select
    date_format(pay_date, '%Y-%m') month_date,
    dish_id,
    dish_name,
    order_count,
    slaves_volume,
    total_sales
from ads_dish_name_1day
where pay_date = date_add('2016-09-01', -1);


-- 历史各菜品销售情况的统计指标
drop table if exists ads_dish_name;
create table if not exists ads_dish_name(
    `dish_id` bigint comment '菜品id',
    `dish_name` varchar(64) comment '菜品名称',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销售量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`dish_id`, `dish_name`)
distributed by hash(`dish_id`) buckets 1
properties(
    "replication_num" = "1"
);


-- 首日装载
insert into ads_dish_name
select
    dish_id,
    dish_name,
    sum(order_count),
    sum(slaves_volume),
    sum(total_sales)
from ads_dish_name_month
group by dish_id, dish_name;


-- 每日装载
# insert into ads_dish_name
select
    dish_id,
    dish_name,
    order_count,
    slaves_volume,
    total_sales
from ads_dish_name_1day
where pay_date = date_add('2016-01-09', -1);



-- 每日口味销售情况的统计指标
drop table if exists ads_flavor_1day;
create table if not exists ads_flavor_1day(
    `pay_date` date comment '支付时间',
    `flavor` varchar(32) comment '口味',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销售量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`pay_date`, `flavor`)
distributed by hash(`pay_date`) buckets 1
properties(
    "replication_num" = "1"
);


-- 首日装载
insert into ads_flavor_1day
select
    pay_date,
    flavor,
    sum(order_count),
    sum(slaves_volume),
    sum(total_sales)
from dws_dish_stat
group by pay_date, flavor;


-- 每日装载
# insert into ads_flavor_1day
select
    pay_date,
    flavor,
    order_count,
    slaves_volume,
    total_sales
from dws_dish_stat
where pay_date = date_add('2016-09-01', -1)



-- 每月口味销售情况的统计指标
drop table if exists ads_flavor_month;
create table if not exists ads_flavor_month(
    `month_date` varchar(10) comment 'yyyy-MM',
    `flavor` varchar(32) comment '口味',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销售量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`month_date`, `flavor`)
distributed by hash(`month_date`) buckets 1
properties(
    "replication_num" = "1"
);



-- 首日装载
insert into ads_flavor_month
select
    date_format(pay_date, '%Y-%m') month_date,
    flavor,
    sum(order_count),
    sum(slaves_volume),
    sum(total_sales)
from ads_flavor_1day
group by date_format(pay_date, '%Y-%m'), flavor;


-- 每日装载
# insert into ads_flavor_month
select
    date_format(pay_date, '%Y-%m') month_date,
    flavor,
    order_count,
    slaves_volume,
    total_sales
from ads_flavor_1day
-- 每日增量数据
where pay_date = date_add('2016-09-01', -1);



-- 历史口味销售情况的统计指标
drop table if exists ads_flavor;
create table if not exists ads_flavor(
    `flavor` varchar(32) comment '口味',
    `order_count` int sum default '0' comment '订单量',
    `slaves_volume` int sum default '0' comment '销售量',
    `total_sales` decimal(16, 2) sum default '0' comment '销售额'
)
aggregate key(`flavor`)
distributed by hash(`flavor`) buckets 1
properties(
    "replication_num" = "1"
);


-- 首日装载
insert into ads_flavor
select
    flavor,
    sum(order_count),
    sum(slaves_volume),
    sum(total_sales)
from ads_flavor_month
group by flavor;


-- 每日装载
# insert into ads_flavor
select
    flavor,
    order_count,
    slaves_volume,
    total_sales
from ads_flavor_1day
where pay_date = date_add('2016-09-01', -1);



-- 每月各类品各菜品销售占比统计
drop table if exists ads_dish_category_name_month;
create table if not exists ads_dish_category_name_month(
    `date_month` varchar(10) comment 'yyyy-MM',
    `dish_category` varchar(32) comment '品类',
    `dish_name` varchar(64) comment '菜品',
    `total_sales` decimal(16, 2) replace default '0' comment '销售额',
    `total_sales_sum` decimal(16, 2) replace default '0' comment '品类总销售额',
    `proportion` decimal(16, 2) replace default '0' comment '占比'
)
aggregate key(`date_month`, `dish_category`, `dish_name`)
distributed by hash(`date_month`) buckets 1
properties(
    "replication_num" = "1"
);


-- 首日装载
insert into ads_dish_category_name_month
select
    date_month,
    dish_category,
    dish_name,
    total_sales,
    total_sales_sum,
    round((total_sales/total_sales_sum) * 100, 2)
from (
    select
        date_month,
        dish_category,
        dish_name,
        total_sales,
        sum(total_sales) over(partition by date_month, dish_category)  total_sales_sum
    from (
        select
            date_format(pay_date, '%Y-%m') date_month,
            dish_category,
            dish_name,
            sum(total_sales) total_sales
        from dws_dish_stat
        group by date_format(pay_date, '%Y-%m'), dish_category, dish_name
    ) tb1
) tb2;



-- 每日装载
# insert into ads_dish_category_name_month
select
    date_month,
    dish_category,
    dish_name,
    total_sales,
    total_sales_sum,
    round((total_sales/total_sales_sum) * 100, 2)
from (
    select
        date_month,
        dish_category,
        dish_name,
        total_sales,
        sum(total_sales) over(partition by dish_category) total_sales_sum
    from (
        select
            date_month,
            dish_category,
            dish_name,
            sum(total_sales) total_sales
        from (
            -- 当月的数据
            select
                date_month,
                dish_category,
                dish_name,
                total_sales
            from ads_dish_category_name_month
            where date_month = date_format('2016-08-30', '%Y-%m')
            union
            -- 当日新增数据
            select
                date_format(pay_date, '%Y-%m') date_month,
                dish_category,
                dish_name,
                sum(total_sales) total_sales
            from dws_dish_stat
            where pay_date = date_add('2016-09-01', -1)
            group by date_format(pay_date, '%Y-%m'), dish_category, dish_name
        ) tb1
        group by date_month, dish_category, dish_name
    ) tb2
) tb3;