from pyspark.sql import SparkSession
from pyspark.sql import function as F
from pyspark.sql.types import TimestampType, IntegerType

def transform_data(spark):
    ## read data
    df = spark.read.csv("gs://raw-youtube-twoset/raw_youtube_csv.csv", header = True)

    ## remove T and Z in publish
    df_new = df.withColumn("publish", F.regexp_replace("publish", 'T', " "))\
                .withColumn("publish", F.regexp_replace("publish", 'Z', ""))

    ## change dtype
    df_transform = df_new.withColumn("view_count", df_new['view'].cast(IntegerType()))\
                    .withColumn("like_count", df_new['like'].cast(IntegerType()))\
                    .withColumn("comments_count", df_new['comments'].cast(IntegerType()))\
                    .withColumn("publish", df_new['publish'].cast(TimestampType()))\
                    .drop("view", "like", "comments")

    ## get hour and date
    df_transformed = df_transform.withColumn("date", F.to_date("publish"))\
                        .withColumn("hour", F.hour("publish"))

    ## change category
    df_list = spark.read.csv("gs://raw-youtube-twoset/youtube_category_list.csv", header=True)
    df_list = df_list.withColumnRenamed('title', 'yt_category')
    df_full = df_test.join(df_list, df_test.category == df_list.id, 'left')\
            .drop(df_test.category)

    ## write to gcs
    df_full.coalease(1).write.csv("gs://processed-youtube-twoset/result_youtube_csv.csv", header = True)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DE Twoset").getOrCreate()
    transform_data(spark)

    spark.stop()