from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, add_months, unix_timestamp, current_timestamp, \
    when, udf, explode, split, trim, lower, regexp_replace, length
from pyspark.sql.types import IntegerType

spark = SparkSession \
    .builder \
    .appName("tp_github_history")\
    .config("spark.task.cpus", 4)\
    .config("spark.dynamicAllocation.enabled", True)\
    .config("spark.dynamicAllocation.minExecutors", 2)\
    .config("spark.dynamicAllocation.maxExecutors", 5)\
    .config("spark.executor.memory", "8g")\
    .config("spark.executor.cores", 4) \
    .master("local[*]")\
    .getOrCreate()


mnm_file = "data/full.csv"

df = spark.read.format("csv") \
    .option("wholeFile", True) \
    .option("multiline", True) \
    .option("header", True) \
    .option("inferSchema", True) \
    .load(mnm_file)


def exo1():
    # print("| exo 1 |")
    # tokenizer = Tokenizer(inputCol="message", outputCol="tokens")
    # tokens = tokenizer.transform(df)
    # tokens.show(truncate=False)

    df.groupby("repo").count().filter("repo is not NULL").sort(desc("count")).limit(10).show(truncate=False)

    return input(
        "Souhaitez-vous avancer à l excerice suivant ? (0 pour exit | n importe quelle touche pour continuer) : ")


def exo2():
    print("| exo 2 |")

    df.groupby("repo", "author").count().filter("repo == 'apache/spark'").sort(desc("count")).limit(1).show(
        truncate=False)

    return input(
        "Souhaitez-vous avancer à l excerice suivant ? (0 pour exit | n importe quelle touche pour continuer) : ")


def exo3():
    # La request :
    print("| exo 3 |")

    len_of_date_format = len("EEE MMM dd HH:mm:ss YYYY") - 4
    last_month_to_search = 24
    date_to_compare = add_months(current_timestamp(), -last_month_to_search).cast("timestamp").cast("long")

    df.select("repo", "author", "date") \
        .where(col("repo") == "apache/spark") \
        .withColumn(
        "substr_date",
        col("date").substr(5, len_of_date_format)
    ) \
        .withColumn(
        "trim_date", when(unix_timestamp(col("substr_date"), "MMM d HH:mm:ss yyyy ").isNotNull(),
                          unix_timestamp(col("substr_date"), "MMM d HH:mm:ss yyyy "))
            .when(unix_timestamp(col("substr_date"), "MMM dd HH:mm:ss yyyy").isNotNull(),
                  unix_timestamp(col("substr_date"), "MMM dd HH:mm:ss yyyy"))
            .otherwise(None)
    ) \
        .where(col("trim_date").isNotNull()) \
        .where((col("trim_date") >= date_to_compare)) \
        .groupby("repo", "author").count() \
        .sort(desc(col("count"))) \
        .show(n=50, truncate=40)

    return input(
        "Souhaitez-vous avancer à l excerice suivant ? (0 pour exit | n importe quelle touche pour continuer) : ")


def exo4():
    print("| exo 4 |")

    df_words = df.select('message')
    # tokenizer = Tokenizer(inputCol="message", outputCol="words")
    # df_tokenized = tokenizer.transform(df_words)
    #
    # df_invalid_words = spark.read.text("./data/englishST.txt")
    #
    # invalid_words = [row['value'] for row in df_invalid_words.collect()]
    #
    # remover = StopWordsRemover(stopWords=invalid_words, inputCol="words", outputCol="messageFiltred")
    #
    # df_filtred = remover.transform(df_tokenized).select('messageFiltred')

    def removePunctuation(column):
        removedSpecialChar = trim(lower(regexp_replace(column, '\W+', ' ').alias('message')))
        return regexp_replace(removedSpecialChar, '[(\s\W)+]', ' ').alias('message')

    # def word_count(str):
    #     counts = dict()
    #     words = str.split(' ')
    #
    #     print(words)
    #     for word in words:
    #         if word in counts:
    #             counts[word] += 1
    #         else:
    #             counts[word] = 1
    #
    #     print(counts)
    #     return counts
    #
    df_wordsTrimmed = df_words.select(removePunctuation(col('message')))

    # df_wordsTrimmed = df_words.select(removePunctuation(col('message')))
    #
    # rddWords = df_wordsTrimmed.rdd.take(5)
    # print(rddWords)

    response4 = df_wordsTrimmed.select(explode(split(col('message'), ' ')).alias('message'))\
        .filter(length(col('message')) > 2).groupBy('message').count()

    response4.show()
    # df_wordsTrimmed.select(explode(split(col('message'), ' ')).alias('word')).groupby('word').count().orderBy("count", ascending=False).limit(10).show()
    # df_filtred.show()

    # df_filtred.withColumn("test", explode('messageFiltred')).select('test').limit(336).show(n=336)


    return


def exos():
    # if exo1() == 0:
    #     exit()
    #
    # if exo2() == 0:
    #     exit()
    #
    # if exo3() == 0:
    #     exit()

    if exo4() == 0:
        exit()

    input("input de fin")
    return


if __name__ == '__main__':
    exos()
