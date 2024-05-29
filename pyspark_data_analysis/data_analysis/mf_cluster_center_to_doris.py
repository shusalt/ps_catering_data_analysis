from pyspark.ml.clustering import KMeansModel
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

if __name__ == '__main__':
    """
    将会员口味偏好聚类中心 overwrite doris
    """
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("doris_conn") \
        .getOrCreate()

    # 读取flavor_pre_model
    path = "file:\\D:\\study-file\\学习笔记\\笔记\\数据仓库项目\\私房小站餐饮数据分析\\相关文件\\机器学习模型\\会员口味偏好聚类分析_Kmeans模型"
    flavor_pre_model = KMeansModel.load(path)

    k = flavor_pre_model.getK()
    feature_cols = ["淡", "甜", "辣", "酸", "香", "鲜"]
    clusters_label_num = [i for i in range(flavor_pre_model.getK())]
    centers = flavor_pre_model.clusterCenters()
    clusters_label = ['会员群' + str(i) for i in range(flavor_pre_model.getK())]
    print(pd.DataFrame(centers))
    df = pd.DataFrame(np.array(centers).T, columns=clusters_label)
    df['feature_cols'] = feature_cols
    print(df)

    # 保存到doris中
    spark_df = spark.createDataFrame(df)
    print(spark_df.show(10, truncate=False))
    spark_df.select("feature_cols", "会员群0", "会员群1", "会员群2", "会员群3", "会员群4", "会员群5").write \
        .format("doris") \
        .option("doris.table.identifier", "private_station.mfp_cluster_analysis_result") \
        .option("doris.fenodes", "172.20.10.3:8070") \
        .option("user", "root") \
        .option("password", "") \
        .option("save_mode", "overwrite") \
        .save()

    # path = "D:\\study-file\\学习笔记\\笔记\\数据仓库项目\\私房小站餐饮数据分析\\相关文件\\mfp_cluster_analysis_result.csv"
    # df.to_csv(path, index=False, encoding="gbk")

    # 绘制雷达图
    # # 保证数据闭合
    # df = pd.concat([df, df[['淡']]], axis=1)
    # centers = np.array(df.iloc[:, 0:])
    # angle1 = np.linspace(0, 2*np.pi, len(feature_cols), endpoint=False)
    # angle2 = np.concatenate((angle1, [angle1[0]]))
    #
    # fig = plt.figure(figsize=(8, 6))
    # ax = fig.add_subplot(111, polar=True)
    # # 显示中文正常
    # plt.rcParams['font.sans-serif'] = ['SimHei']
    # plt.rcParams['axes.unicode_minus'] = False
    #
    # for i in range(k):
    #     ax.plot(angle2, centers[i], 'o-', linewidth=2)
    #     # 填充
    #     # ax.fill(angle2, centers[i], alpha=0.3)
    #
    # # 添加属性标签
    # ax.set_thetagrids(angle1 * 180 / np.pi, labels=feature_cols)
    # plt.legend(clusters_label)
    # plt.show()

    # x = np.linspace(0, 2*np.pi, 6, endpoint=False)
    # y = df.iloc[0:6, 1].tolist()
    #
    # fig = plt.figure(figsize=(5, 5))
    # x = np.concatenate((x, [x[0]]))
    # y = np.concatenate((y, [y[0]]))
    # print(x)
    # print(y)

    # fig = plt.figure(figsize=(6, 6))
    #
    # x = np.linspace(0, 2 * np.pi, 6, endpoint=False)
    # y = [83, 61, 95, 67, 76, 88]
    #
    # # 保证首位相连
    # x = np.concatenate((x, [x[0]]))
    # y = np.concatenate((y, [y[0]]))
    # print(x)
    # print(y)
    #
    # # 雷达图
    # axes = plt.subplot(111, polar=True)
    #
    # axes.plot(x, y, 'o-', linewidth=2)  # 连线
    # axes.fill(x, y, alpha=0.3)  # 填充
    #
    # # 显示刻度
    # plt.show()
