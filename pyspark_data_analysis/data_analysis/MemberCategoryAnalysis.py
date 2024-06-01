import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt

from data_analysis.comm.SparkConnDoris import SparkConnDoris

if __name__ == '__main__':
    """
    会员品类偏好聚类分析
    """
    # 读取数据
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("p1") \
        .getOrCreate()

    member_category_pre_df = spark.read.format("doris") \
        .option("doris.table.identifier", "private_station.da_member_category_pre") \
        .option("doris.fenodes", "172.20.10.3:8070") \
        .option("user", "root") \
        .option("password", "") \
        .load()

    # 行转列
    member_category_pre_df2 = member_category_pre_df \
        .groupby("member_id", "member_name") \
        .pivot("base_category") \
        .agg(first("composite_category"))

    # 特征向量化
    feature_cols = ["主食类", "小吃类", "果蔬类", "海_河鲜类", "肉类", "酒类", "饮料类"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    member_category_pre_df2 = assembler.transform(member_category_pre_df2)

    # 特征数据标准化(归一化)
    standardscaler = StandardScaler(inputCol="features", outputCol="sc_features", withMean=True, withStd=True)
    scaler_mode = standardscaler.fit(member_category_pre_df2)
    member_category_pre_df2 = scaler_mode.transform(member_category_pre_df2)

    # # 使用Kmeans聚类方法，对会员进行聚类分析
    # # 使用肘部法，确定最佳k值
    # cost = []  # 剪影值列表
    # # 构建聚类算法评估器, 使用ClusterEvaluator
    # # featuresCol: 特征列, metricName: 指标名称（采用剪影轮廓），distanceMeasure：距离计算的方法（采用平方欧几里得）
    # evaluator = ClusteringEvaluator(featuresCol="sc_features",
    #                                 metricName='silhouette',
    #                                 distanceMeasure='squaredEuclidean')
    #
    # for k in range(2, 10):
    #     # featuresCol: 特征列，k: 簇类数 seed: 随机种子
    #     kmeans = KMeans(featuresCol="sc_features", k=k, seed=1)
    #     kmeans_model = kmeans.fit(member_category_pre_df2)  # 训练
    #     predictions_df = kmeans_model.transform(member_category_pre_df2)  # 预测
    #     # 评估模型
    #     silhouette = evaluator.evaluate(predictions_df)
    #     cost.append((k, silhouette))
    #
    # print(cost)
    #
    # # 绘制肘部图，寻找手肘部分
    # x, y = zip(*cost)
    # plt.figure(figsize=(5, 5))
    # plt.plot(x, y, marker='o')
    # plt.show()

    # 通过绘图可知，选取k=4
    kmeans = KMeans(featuresCol="sc_features", k=4, seed=1)
    kmeans_model = kmeans.fit(member_category_pre_df2)
    predictions_df = kmeans_model.transform(member_category_pre_df2)
    # # 模型保存
    # path = 'file:\\D:\\study-file\\学习笔记\\笔记\\数据仓库项目\\私房小站餐饮数据分析\\相关文件\\机器学习模型\\会员品类偏好聚类分析_Kmeans模型'
    # kmeans_model.save(path)
    print(predictions_df.show())

    # 将predictions_df 写入 doris
    predictions_df.select("member_id", "member_name", "主食类", "小吃类", "果蔬类", "海_河鲜类",
                          "肉类", "酒类", "饮料类", "prediction") \
        .write.format("doris") \
        .option("doris.table.identifier", "private_station.da_member_category_predictions") \
        .option("doris.fenodes", "172.20.10.3:8070") \
        .option("user", "root") \
        .option("password", "") \
        .option("save_mode", "overwrite") \
        .save()

    # 各聚类中心结果
    cluster_centers = kmeans_model.clusterCenters()
    cluster_class = ['member_cluster' + str(i) for i in range(kmeans_model.getK())]
    # 为了方便雷达图可视分析，对其pd_df进行转置
    pd_df = pd.DataFrame(np.array(cluster_centers).T, columns=cluster_class)
    pd_df['features'] = feature_cols
    # 将pd_df to spark_df
    mc_cluster_center_analysis_result = spark.createDataFrame(pd_df)
    mc_cluster_center_analysis_result2 = mc_cluster_center_analysis_result \
        .select("features", "member_cluster0", "member_cluster1",
                "member_cluster2", "member_cluster3")
    #  将结果写入 doris
    print(mc_cluster_center_analysis_result2.show())
    spark_to_doris = SparkConnDoris(mc_cluster_center_analysis_result2, "mc_cluster_center_analysis_result")
    spark_to_doris.write_doris()
    # mc_cluster_center_analysis_result2.write.format("doris") \
    #     .option("doris.table.identifier", "private_station.mc_cluster_center_analysis_result") \
    #     .option("doris.fenodes", "172.20.10.3:8070") \
    #     .option("user", "root") \
    #     .option("password", "") \
    #     .option("save_mode", "overwrite") \
    #     .save()
