from pyspark.sql import SparkSession, SQLContext
import boto3

spark = SparkSession.builder\
     .master("spark://ec2-34-199-62-71.compute-1.amazonaws.com:7077")\
     .appName("consumer-insights")\
     .config("spark.executor.memory", "6gb")\
     .getOrCreate()

# spark = SparkSession.builder\
#     .appName("consumer-insights")\
#     .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)

s3 = boto3.client('s3')
response = s3.list_objects_v2(Bucket='amazon-customer-reviews-dataset', Prefix='parquet/', Delimiter='/')

departments = []
for obj in response.get('CommonPrefixes'):
    department = str(obj.get('Prefix')).replace("parquet/product_category=", "")
    departments.append(department)

# reviews = sqlContext.read.parquet('../../sample_input/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')

for department in departments:
    reviews = sqlContext.read.parquet('s3n://amazon-customer-reviews-dataset/parquet/product_category='+department)
    print(department)
    for year in range(1999, 2016):
        reviews_year = reviews.filter(reviews.year == year)
        reviews_year.write.parquet("s3a://amazon-customer-reviews-dataset/ts/"+str(year)+"/"+department, "append")
        print(year)

print("Finished")
