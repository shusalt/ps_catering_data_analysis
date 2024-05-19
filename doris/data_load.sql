create database private_station;

-- order_info 订单信息表
drop table if exists order_info;
create table if not exists order_info(
    `order_id` bigint comment '订单号',
    `member_name` varchar(10) comment '会员名',
    `shop_name` varchar(32) comment '店铺名',
    `shop_location` varchar(10) comment '店铺所在地',
    `order_time` datetime comment '点餐时间',
    `consumption_amount` decimal(16, 2) comment '消费金额',
    `is_paid` int comment '是否结算：0.未结算.1.已结算',
    `payment_time` datetime comment '结算时间'
)
duplicate key(`order_id`, `member_name`)
distributed by hash(`order_id`) buckets 1
properties ("replication_num" = "1");


-- order_detail 订单明细表
drop table if exists order_detail;
create table if not exists order_detail(
    `order_id` bigint comment '订单号',
    `dish_name` varchar(32) comment '菜品名称',
    `price` decimal(16, 2) comment '价格',
    `quantity` int comment '数量',
    `detail_date` string comment '日期',
    `detail_time` string comment '时间'
)
duplicate key(`order_id`, `dish_name`)
distributed by hash(`order_id`) buckets 1
properties ("replication_num" = "1");



-- dish_info 菜品信息表
