from pyspark.sql import SparkSession
from pyspark.sql.functions import col,round
import pandas as pd
import os
os.system('chcp 65001') # 解决乱码 explicitly changed encoding to utf-8
'''
将Excel使用spark处理，并上传到MySQL
确诊详情数据字段列表
'''
def pandas_to_df():
    # pandas 读取Excel文件
    excel_file = pd.read_excel(r"G:\信息系统分析课设\data\中国本土疫情数据.xlsx")
    # 删除包含空值的行
    df = excel_file.dropna()
    return df

def spark_session(df):
    # 创建SparkSession对象,内存配置为6g，驱动器内存配置为4g
    spark = SparkSession.builder \
        .appName("PySpark CSV Example") \
        .config("spark.executor.memory", "3g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    # 将pandas DataFrame转换为Spark DataFrame
    spark_df = spark.createDataFrame(df)
    return spark_df

def data_process(spark_df):
    # 数据处理
    # 1.重命名列名
    renamed_df = spark_df.withColumnRenamed("新增境外输入", "境外输入") \
        .withColumnRenamed("新增死亡", "死亡人数") \
        .withColumnRenamed("新增治愈", "治愈人数") \
        .withColumnRenamed("新增无症状", "无症状感染数")

    # 2.计算数据
    # 计算死亡率
    death_rate_df = renamed_df.withColumn("死亡率", round(col("累计死亡") / col("累计确诊") * 100, 2))
    # 计算治愈率
    new_df = death_rate_df.withColumn("治愈率", round(col("累计治愈") / col("累计确诊") * 100, 2))

    # 3.数据转换
    select_df = new_df.select(
        col("日期").cast("string"),
        col("境外输入").cast("int"),
        col("死亡人数").cast("int"),
        col("现有确诊").cast("int"),
        col("治愈人数").cast("int"),
        col("累计确诊").cast("int"),
        col("死亡率").cast("double"),
        col("治愈率").cast("double"),
        col("无症状感染数").cast("int")
    )

    # 显示Spark DataFrame
    # select_df.show()
    # select_df.printSchema()
    print("spark数据处理完成！")
    return select_df

def spark_to_mysql(select_df):
    # 数据保存到mysql，配置MySQL连接信息
    mysql_host = "localhost"
    mysql_port = "3306"
    mysql_database = "yiqing"
    mysql_username = "root"
    mysql_password = "123456"

    # 创建MySQL连接URL
    mysql_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"

    # 配置MySQL连接器属性
    mysql_properties = {
        "user": mysql_username,
        "password": mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # 将DataFrame保存到MySQL表中
    select_df.write.jdbc(mysql_url, "confirm_details_data", mode="overwrite", properties=mysql_properties)
    print("MySQL上传完成！")
def main():
    df = pandas_to_df()
    spark_df = spark_session(df)
    select_df = data_process(spark_df)
    spark_to_mysql(select_df)

if __name__ == "__main__":
    main()
