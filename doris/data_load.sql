create database private_station;

-- order_info 订单信息表
drop table if exists order_info;
create table if not exists order_info(
    `order_id` bigint comment '订单号',
    `member_name` varchar(10) comment '会员名',
    `shop_name` varchar(32),
    `shop_location` varchar(10),
    `order_time` date
);


select unix_timestamp();