from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.clustering import KMeans

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("doris_conn") \
        .getOrCreate()

    # 读取会员口味偏好表
    member_flavor_pre = spark.read.format("doris")\
        .option("doris.table.identifier", "private_station.data_analysis_member_flavor_pre")\
        .option("doris.fenodes", "172.20.10.3:8070")\
        .option("user", "root")\
        .option("password", "")\
        .load()

    print(member_flavor_pre.show(10, truncate=False))

    # 口味特征，行转列操作
    # 列名：各个基础口味 列值：各个基础口味的消费次数
    member_flavor_pre2 = member_flavor_pre\
        .groupby("member_id", "member_name")\
        .pivot("base_flavor")\
        .agg(first("consumption_count"))

    print(member_flavor_pre2.show(20, truncate=False))
