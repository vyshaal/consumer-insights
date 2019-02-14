from pyspark.sql import SparkSession, SQLContext

# spark = SparkSession.builder\
#     .master("spark://ec2-34-199-62-71.compute-1.amazonaws.com:7077")\
#     .appName("consumer-insights")\
#     .config("spark.executor.memory", "5gb")\
#     .getOrCreate()

spark = SparkSession.builder\
    .appName("consumer-insights")\
    .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)
# reviews = sqlContext.read.parquet('s3n://amazon-customer-reviews-dataset/parquet/product_category=Furniture/*')
reviews = sqlContext.read.parquet('../../sample_input/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')


for year in range(1999, 2015):
    reviews = reviews.filter(reviews.year == year).sort("review_date")
    # reviews.write.parquet("s3a://amazon-customer-reviews-dataset/year/"+str(year)+"/", mode="overwrite")
    print(year)

print("Finished")