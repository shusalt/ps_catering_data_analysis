from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt
import pandas as pd

if __name__ == '__main__':
    """
    会员口味偏好聚类分析
    """
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("doris_conn") \
        .getOrCreate()

    # 读取会员口味偏好表
    member_flavor_pre = spark.read.format("doris") \
        .option("doris.table.identifier", "private_station.data_analysis_member_flavor_pre") \
        .option("doris.fenodes", "172.20.10.3:8070") \
        .option("user", "root") \
        .option("password", "") \
        .load()
    print(member_flavor_pre.show(10, truncate=False))

    # 口味特征，行转列操作
    # 列名：各个基础口味 列值：各个基础口味的消费次数
    member_flavor_pre2 = member_flavor_pre \
        .groupby("member_id", "member_name") \
        .pivot("base_flavor") \
        .agg(first("consumption_count"))
    print(member_flavor_pre2.show(20, truncate=False))

    # 组装特征向量
    feature_cols = ["淡", "甜", "辣", "酸", "香", "鲜"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    member_flavor_pre2 = assembler.transform(member_flavor_pre2)
    print(member_flavor_pre2.show(20, truncate=False))

    # 标准化(归一化)特征数据
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=True)
    scaler_model = scaler.fit(member_flavor_pre2)
    member_flavor_pre2 = scaler_model.transform(member_flavor_pre2)
    print(member_flavor_pre2.show(20, truncate=False))

    # # 使用肘部法或轮廓系数等方法确定最佳聚类数目
    # # 这里使用的是肘部法
    # cost = []  # silhouette(剪影)列表
    # # evaluator评估器
    # evaluator = ClusteringEvaluator(featuresCol='scaled_features',
    #                                 metricName='silhouette',
    #                                 distanceMeasure='squaredEuclidean')
    #
    # # 寻找最佳的聚类簇数
    # for k in range(2, 20):
    #     kmeans = KMeans(featuresCol='scaled_features', k=k, seed=1)
    #     kmeans_model = kmeans.fit(member_flavor_pre2)
    #     predictions_df = kmeans_model.transform(member_flavor_pre2)
    #     silhouette = evaluator.evaluate(predictions_df)
    #     cost.append(silhouette)
    # print(cost)
    #
    # # 绘制肘部法图形
    # x, y = zip(*cost)
    # plt.figure(figsize=(10, 6))
    # plt.plot(x, y, marker='o')
    # plt.xlabel('Number of clusters')
    # plt.ylabel('Silhouette Score')
    # plt.title('Elbow Method For Optimal k')
    # plt.show()

    # 根据肘部发，选取k=6
    k = 6
    kmeans = KMeans(featuresCol='scaled_features', k=6, seed=1)
    flavor_pre_model = kmeans.fit(member_flavor_pre2)
    predictions_df = flavor_pre_model.transform(member_flavor_pre2)
    print(predictions_df
          .select("member_id", "member_name", "淡", "甜", "辣", "酸", "香", "鲜", "prediction")
          .show(truncate=False))

    # 将聚类分析结果保存到doris中
    predictions_df.select("member_id", "member_name", "淡", "甜", "辣", "酸", "香", "鲜", "prediction").write \
        .format("doris") \
        .option("doris.table.identifier", "private_station.da_member_flavor_predictions") \
        .option("doris.fenodes", "172.20.10.3:8070") \
        .option("user", "root") \
        .option("password", "") \
        .option("save_mode", "overwrite") \
        .save()

    # # 保存模型
    # path = "file:\\D:\\study-file\\学习笔记\\笔记\\数据仓库项目\\私房小站餐饮数据分析\\相关文件\\机器学习模型\\会员口味偏好聚类分析_Kmeans模型"
    # flavor_pre_model.save(path)
    #
    # # 获取簇类中心
    # centers = flavor_pre_model.clusterCenters()
    # print(pd.DataFrame(centers, columns=feature_cols))
