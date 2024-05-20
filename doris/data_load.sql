# create database private_station;

-- ods_order_info 订单信息表
drop table if exists ods_order_info;
create table if not exists ods_order_info(
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


-- ods_order_detail 订单明细表
drop table if exists ods_order_detail;
create table if not exists ods_order_detail(
    `order_id` bigint comment '订单号',
    `dish_name` varchar(64) comment '菜品名称',
    `price` decimal(16, 2) comment '价格',
    `quantity` int comment '数量',
    `detail_date` string comment '日期',
    `detail_time` string comment '时间'
)
duplicate key(`order_id`, `dish_name`)
distributed by hash(`order_id`) buckets 1
properties ("replication_num" = "1");



-- ods_dish_info 菜品信息表
drop table if exists ods_dish_info;
create table if not exists ods_dish_info(
    `dish_id` bigint comment '菜品id',
    `dish_name` varchar(64) comment '菜品名称',
    `flavor` varchar(10) comment '菜品口味',
    `price` decimal(16, 2) comment '价格',
    `cost` decimal(16, 2) comment '成本',
    `recommendation_level` float comment '推荐度',
    `dish_category` varchar(20) comment '菜品类别'
)
duplicate key(`dish_id`, `dish_name`)
distributed by hash(`dish_id`) buckets 1
properties("replication_num" = "1");



-- ods_member_info 会员信息表
drop table if exists ods_member_info;
create table if not exists ods_member_info(
    `member_id` bigint comment '会员号',
    `member_name` varchar(10) comment '会员名',
    `gender` varchar(3) comment '性别',
    `age` int comment '年龄',
    `member_join_date` datetime comment '入会时间',
    `phone_number` bigint comment '手机号',
    `membership_level` varchar(10) comment '会员等级'
)
duplicate key(`member_id`, `member_name`)
distributed by hash(`member_id`) buckets 1
properties("replication_num" = "1");



# -- 数据装载，采用stream_load方式
# -- ods_order_info数据加载
# curl --location-trusted -u root: \
#     -H "Expect:100-continue" \
#     -H "column_separator:," \
#     -H "columns:order_id, member_name, shop_name, shop_location, order_time, consumption_amount, is_paid, payment_time" \
#     -T 订单信息表.csv \
#     -XPUT http://172.20.10.3:8070/api/private_station/ods_order_info/_stream_load
#
#
# -- ods_order_detail数据加载
# curl --location-trusted -u root: \
#     -H "Expect:100-continue" \
#     -H "column_separator:," \
#     -H "columns:order_id, dish_name, price, quantity, detail_date, detail_time" \
#     -T 订单详情表.csv \
#     -XPUT http://172.20.10.3:8070/api/private_station/ods_order_detail/_stream_load
#
#
#
# -- ods_dish_info数据加载
# curl --location-trusted -u root: \
#     -H "Expect:100-continue" \
#     -H "column_separator:," \
#     -H "columns:dish_id, dish_name, flavor, price, cost, recommendation_level, dish_category" \
#     -T 菜品信息表.csv \
#     -XPUT http://172.20.10.3:8070/api/private_station/ods_dish_info/_stream_load
#
#
#
# -- ods_member_info
# curl --location-trusted -u root: \
#     -H "Expect:100-continue" \
#     -H "column_separator:," \
#     -H "columns:member_id, member_name, gender, age, member_join_date, phone_number, membership_level" \
#     -T 会员信息表.csv\
#     -XPUT http://172.20.10.3:8070/api/private_station/ods_member_info/_stream_load