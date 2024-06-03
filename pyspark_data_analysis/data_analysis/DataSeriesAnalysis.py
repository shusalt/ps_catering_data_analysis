from pyspark.sql import SparkSession
import pandas as pd
import numpy as np

if __name__ == '__main__':
    """
    时间序列分析，
    预测未来7天的销量
    """
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("DataSeriesAnalysis") \
        .getOrCreate()

    df = spark.read.format("doris") \
        .option("doris.table.identifier", "private_station.da_date_series_amount_analysis") \
        .option("doris.fenodes", "172.20.10.3:8070") \
        .option("user", "root") \
        .option("password", "") \
        .load()

    # 将df转换为pd_df
    pd_df = df.toPandas()
    # 设置日期列为索引
    pd_df.set_index('date', inplace=True)
    pd_df.sort_index(inplace=True)

    # 首先进行ADF(单位根)检验
    from statsmodels.tsa.stattools import adfuller
    from numpy import log

    # result1 = adfuller(pd_df['quantity'].dropna())
    # print("ADF Statistic:{}".format(result1[0]))
    # print("p-value:{}".format(result1[1]))
    # result2 = adfuller(pd_df['total_sales'].dropna())
    # print("ADF Statistic:{}".format(result2[0]))
    # print("p-value:{}".format(result2[1]))

    # 引入绘图库
    # 绘制原始时序图和自相关图、差分时序图和差分自相关图，找出合适的差分i
    import matplotlib.pyplot as plt
    from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

    # fig1, axes1 = plt.subplots(3, 2, sharex=True)
    # # 原始 quantity列
    # axes1[0, 0].plot(pd_df['quantity'].to_numpy())  # quantity原始时序图
    # axes1[0, 0].set_title("Quantity Original Series")
    # plot_acf(pd_df['quantity'].to_numpy(), ax=axes1[0, 1])  # quantity原始自相关图
    #
    # # 一阶差分
    # axes1[1, 0].plot(pd_df['quantity'].diff().to_numpy())  # quantity时序图
    # axes1[1, 0].set_title("Quantity 1st Order diff")
    # plot_acf(pd_df['quantity'].diff().dropna().to_numpy(), ax=axes1[1, 1])  # quantity自相关图
    #
    # # 二阶差分
    # axes1[2, 0].plot(pd_df['quantity'].diff().diff().to_numpy())  # quantity时序图
    # axes1[2, 0].set_title("Quantity 2st Order diff")
    # plot_acf(pd_df['quantity'].diff().diff().dropna().to_numpy(), ax=axes1[2, 1])  # quantity自相关图
    #
    # fig2, axes2 = plt.subplots(3, 2, sharex=True)
    # # 原始 total_sales
    # axes2[0, 0].plot(pd_df['total_sales'].to_numpy())  # total_sales原始时序图
    # axes2[0, 0].set_title("Total_sales Original Series")
    # plot_acf(pd_df['total_sales'].to_numpy(), ax=axes2[0, 1])  # total_sales原始自相关图
    #
    # # 一阶差分
    # axes2[1, 0].plot(pd_df['total_sales'].diff().to_numpy())  # total_sales时序图
    # axes2[1, 0].set_title("Total_sales 1st Order diff")
    # plot_acf(pd_df['total_sales'].diff().dropna().to_numpy(), ax=axes2[1, 1])  # total_sales自相关图
    #
    # # 二阶差分
    # axes2[2, 0].plot(pd_df['total_sales'].diff().diff().to_numpy())  # total_sales时序图
    # axes2[2, 0].set_title("Total_sales 2st Order diff")
    # plot_acf(pd_df['total_sales'].diff().diff().dropna().to_numpy(), ax=axes2[2, 1])  # total_sales自相关图
    # plt.show()

    # 绘制偏自相关图, 差分设置为1
    # 选取合适的AR(p)的p阶、MA(q)的q阶
    # fig3, axes3 = plt.subplots(2, 2, sharex=True)
    # plot_pacf(pd_df['quantity'].diff().dropna().to_numpy(), ax=axes3[0, 0], title="quantity pacf")
    # plot_pacf(pd_df['total_sales'].diff().dropna().to_numpy(), ax=axes3[0, 1], title="total_sales pacf")
    # plt.show()

    # 根据相关绘图分析，最终定阶为：p:1 i=1 q=0
    # 引入时间序列分析模型，ARIMA 算法
    # tsa: 时间序列分析
    from statsmodels.tsa.arima.model import ARIMA

    # 定义时序模型
    model_quantity = ARIMA(pd_df['quantity'], order=(1, 1, 1)).fit()
    model_total_sales = ARIMA(pd_df['total_sales'], order=(1, 1, 1)).fit()

    print(model_quantity.forecast(7))

    print(model_total_sales.forecast(7))
