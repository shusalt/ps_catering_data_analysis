# 项目概述

本项目皆在为"私房小站"的餐饮数据，构建一个数据仓库，为数据分析，BI报表提供数据支撑，以及通过分析"私房小站"的餐饮数据来探索本企业的餐饮运营情况；

# 数据仓库架构

![数据流程架构图](./image/数据流程架构图.png)

![数据分层图](./image/数据分层图.png)

# 原始业务数据

原始业务表包含有：

- 订单信息表
- 订单详情表
- 菜品信息表
- 会员信息表

订单信息表：

| 字段               | 说明                        |
| ------------------ | --------------------------- |
| order_id           | 订单号                      |
| membee_name        | 会员名                      |
| shop_name          | 店铺名                      |
| shop_location      | 店铺所在地                  |
| order_time         | 点餐时间                    |
| consumption_amount | 消费金额                    |
| is_paid            | 是否结算：0.未结算.1.已结算 |
| payment_time       | 结算时间                    |

订单详情表：

| 字段      | 说明     |
| --------- | -------- |
| order_id  | 订单号   |
| dish_name | 菜品名称 |
| price     | 价格     |
| quantity  | 数量     |
| date      | 日期     |
| time      | 时间     |

菜品信息表：

| 字段                 | 说明     |
| -------------------- | -------- |
| dish_id              | 菜品id   |
| dish_name            | 菜品名称 |
| flavor               | 菜品口味 |
| price                | 价格     |
| cost                 | 成本     |
| recommendation_level | 推荐度   |
| dish_category        | 菜品类别 |

会员信息表：

| 字段                 | 说明     |
| -------------------- | -------- |
| member_id            | 会员号   |
| member_name          | 会员名   |
| gender               | 性别     |
| age                  | 年龄     |
| membership_join_date | 入会时间 |
| phone_number         | 手机号   |
| membership_level     | 会员等级 |

# ODS层比表设计

对与原始业务数据，存储ODS层中，采用Doris的duplicate数据表模型，进行设计

ods_order_info(订单信息表):

| 字段               | 数据类型       | 说明                        |
| ------------------ | -------------- | --------------------------- |
| order_id           | bigint         | 订单号                      |
| member_name        | varchar(10)    | 会员名                      |
| shop_name          | varchar(32)    | 店铺名                      |
| shop_location      | varchar(10)    | 店铺所在地                  |
| order_time         | date           | 点餐时间                    |
| consumption_amount | decimal(16, 2) | 消费金额                    |
| is_paid            | int            | 是否结算：0.未结算.1.已结算 |
| payment_time       | date           | 结算时间                    |

ods_order_detail(订单详情表):

| 字段        | 数据类型       | 说明     |
| ----------- | -------------- | -------- |
| order_id    | bigint         | 订单号   |
| dish_name   | varchar(32)    | 菜品名称 |
| price       | decimal(16, 2) | 价格     |
| quantity    | int            | 数量     |
| detail_date | date           | 日期     |
| detail_time | time           | 时间     |

ods_dish_info(菜品信息表):

| 字段                 | 数据类型       | 说明     |
| -------------------- | -------------- | -------- |
| dish_id              | bigint         | 菜品id   |
| dish_name            | varchar(32)    | 菜品名称 |
| flavor               | varchar(10)    | 菜品口味 |
| price                | decimal(16, 2) | 价格     |
| cost                 | decimal(16, 2) | 成本     |
| recommendation_level | float          | 推荐度   |
| dish_category        | varchar(10)    | 菜品类别 |

ods_member_info(会员信息表):

| 字段                 | 数据类型    | 说明     |
| -------------------- | ----------- | -------- |
| member_id            | bigint      | 会员号   |
| member_name          | varchar(10) | 会员名   |
| gender               | varchar(3)  | 性别     |
| age                  | int         | 年龄     |
| membership_join_date | date        | 入会时间 |
| phone_number         | bigint      | 手机号   |
| membership_level     | varchar(10) | 会员等级 |
