from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import configparser

configs = configparser.ConfigParser()
configs.read('configs.ini')

if __name__ == "__main__":

    btc_data_path = configs["data"]["btc_data_path"]
    db_url = configs["db"]["db_url"]
    db_table = configs["db"]["db_table"]
    user = configs["db"]["user"]
    password = configs["db"]["password"]

    spark = SparkSession.builder\
        .appName("BTC-Volatility-Calculation")\
        .getOrCreate()

    btc_data = spark\
        .read\
        .json(btc_data_path)

    btc_data_transformed = btc_data\
        .withColumn("time_period_start_timestamp", to_date("time_period_start"))
    btc_data_transformed.createOrReplaceTempView("btc_data_transformed")

    btc_daily_volatility_calculated = \
        spark.sql("SELECT sum((price_open-price_close)*(price_open-price_close))/count(*) AS volatility, "
                         + "max(price_high) AS day_high, min(price_low) AS day_low, "
                         + "time_period_start_timestamp AS date "
                         + "FROM btc_data_transformed GROUP BY time_period_start_timestamp "
                         + "ORDER BY time_period_start_timestamp asc")

    btc_daily_volatility_calculated.write.format('jdbc').options(
        url= db_url,
        driver="org.postgresql.Driver",
        dbtable= db_table,
        user=user,
        password=password).mode('append').save()



