from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, split
from pyspark.sql.types import StringType, IntegerType
from pyspark.ml.feature import Tokenizer
import random
from re import search, sub, findall, IGNORECASE, compile
import os
import sys



# Initiate Spark Context (for result output)
sc = SparkContext(appName="cog_complex_score", master="local[2]",
                  conf=SparkConf().set('spark.ui.port',
                                       random.randrange(4000, 5000)))
# Initiate Spark Session (for creating dataframe)
spark = SparkSession.builder.appName('ceo_score')\
    .master("local[2]")\
    .config('spark.ui.port', random.randrange(4000, 5000)).getOrCreate()

with open('dictionary/strong.txt', 'r') as f:
    lst = f.read().splitlines()
    lst = list(filter(None, lst))
    strong_exclude_lst = list(filter(lambda x: x.startswith('-'), lst))
    strong_exclude_lst = list(map(lambda x: x.strip('-'), strong_exclude_lst))
    strong_lst = list(filter(lambda x: not x.startswith('-'), lst))
   # print(strong_exclude_lst)


with open('dictionary/weak.txt', 'r') as f:
    lst = f.read().splitlines()
    lst = list(filter(None, lst))
    weak_exclude_lst = list(filter(lambda x: x.startswith('-'), lst))
    weak_exclude_lst = list(map(lambda x: x.strip('-'), weak_exclude_lst))
    weak_lst = list(filter(lambda x: not x.startswith('-'), lst))


with open('dictionary/comp.txt', 'r') as f:
    lst = f.read().splitlines()
    comp_lst = list(filter(None, lst))


with open('dictionary/diff.txt', 'r') as f:
    lst = f.read().splitlines()
    lst = list(filter(None, lst))
    diff_exclude_lst = list(filter(lambda x: x.startswith('-'), lst))
    diff_exclude_lst = list(map(lambda x: x.strip('-'), diff_exclude_lst))
    diff_lst = list(filter(lambda x: not x.startswith('-'), lst))


def string_found(word_lst, word_exclude_lst, data):

    result = 0
    for words in word_lst:
        if search('\*', words):
            word = sub('\*$', '', words)
            r = compile(word + r'.*')
            detect_duplicate_word = list(filter(r.match, strong_exclude_lst))
            # print(detect_duplicate_word)
            result += len(findall(r'\b' + str(word) + r'(?=.*\b)', data, flags=IGNORECASE))

            for non_word in detect_duplicate_word:
                result -= len(findall(r'\b' + str(non_word) + r'\b', data, flags=IGNORECASE))

        else:
            try:
                result += len(findall(r"\b"+str(words)+r"\b", data, flags=IGNORECASE))
            except:
                result += 0

    return result


def diff_score(data): # data is the column name value (sentence)

    result = string_found(diff_lst, diff_exclude_lst, data)

    return result


def strong_score(data):

    strong_result = string_found(strong_lst, strong_exclude_lst, data)

    return strong_result


def weak_score(data):

    weak_result = string_found(weak_lst, weak_exclude_lst, data)

    return weak_result


def comp_score(data):

    comp_result = string_found(comp_lst, [], data)

    return comp_result

## File directory folder argument input (main)
FILE_DIRECTORY = 'gs://author_data/'+sys.argv[1] 
OUTPUT_DIRECTORY = 'gs://author_data/result/'+sys.argv[2]


#FILE_DIRECTORY = 'ceo_weird_qayearly'
#OUTPUT_DIRECTORY = 'result'

tokenizer = Tokenizer(inputCol="text", outputCol="words")

countTokens = udf(lambda words: len(words), IntegerType())
countDiff = udf(lambda sentence: diff_score(sentence), IntegerType())
countStrong = udf(lambda sentence: strong_score(sentence), IntegerType())
countWeak = udf(lambda sentence: weak_score(sentence), IntegerType())
countComp = udf(lambda sentence: comp_score(sentence), IntegerType())
filenameChange = udf(lambda file: os.path.basename(file), StringType())


total_cores = int(sc._conf.get('spark.executor.instances')) * int(sc._conf.get('spark.executor.cores'))
numPartitions = total_cores * 3

df_numPartitions = numPartitions / 2
df = sc.wholeTextFiles(FILE_DIRECTORY, minPartitions = numPartitions).repartition(int(df_numPartitions)).toDF()
new_df = df.toDF('filename', 'text')

tokenized = tokenizer.transform(new_df)

new_df = tokenized.withColumn('tokens', countTokens(col('words')))\
                  .withColumn('diff_tokens', countDiff(col('text')))\
                  .withColumn('comp_tokens', countComp(col('text')))\
                  .withColumn('strong_tokens', countStrong(col('text')))\
                  .withColumn('weak_tokens', countWeak(col('text'))).drop(col('words')).drop(col('text')).cache()
#new_df = new_df.repartition(30).cache()

# new_df = new_df.groupBy(col('filename')).sum()
new_df = new_df.toDF('ceo_year_name', 'token_count', 'diff_token_count', 'comp_token_count',
                     'strong_token_count', 'weak_token_count').cache()

calculated_df = new_df.withColumn('diff_score', col('diff_token_count') / col('token_count') )
calculated_df = calculated_df.withColumn('nuance_score', (col('weak_token_count') + 0.01) /
                                         (col('weak_token_count') + col('strong_token_count') + 0.02))
calculated_df = calculated_df.withColumn('comp_score', col('comp_token_count') / col('token_count'))
file_change_df = calculated_df.withColumn('ceo_year_name', filenameChange(col('ceo_year_name')))

file_change_df = file_change_df.filter(col('token_count') >= 250)

## IMPORTANT!!!!!!!
# If the program crashes here, you may need to comment this section out from line 155-160. 
# Replace in line 157 
# From: file_name_change_df 
# To: file_change_df
# file_change_df = file_change_df ...

split_col = split(col('ceo_year_name'), '_')
file_name_change_df = file_change_df.withColumn('', split_col.getItem(0)).cache() 
file_name_change_df = file_name_change_df.withColumn('name', split_col.getItem(1))
file_name_change_df = file_name_change_df.withColumn('year', split_col.getItem(2))

file_name_change_df = file_name_change_df.select('ceo_year_name', 'company', 'name', 'year', 'token_count',
                                                 'diff_token_count', 'comp_token_count', 'strong_token_count',
                                                 'weak_token_count', 'diff_score', 'nuance_score', 'comp_score')
file_name_change_df.show()
file_name_change_df.write.mode('overwrite').option('header', 'true').csv(OUTPUT_DIRECTORY)
