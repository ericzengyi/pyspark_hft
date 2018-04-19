# Databricks notebook source
dbutils.fs.rm('file:/dbfs/FileStore/tables/parquet_data/quotes_UsConsolidated_UsListing_AU_Bz_2017_12_11.parquet')
dbutils.fs.rm('file:/dbfs/FileStore/tables/parquet_data/quotes_UsConsolidated_UsListing_CU_Dz_2017_12_11.parquet')
dbutils.fs.mkdirs('file:/dbfs/tmp/parquet_data/')

# COMMAND ----------

import pandas as pd
import pyarrow
import pyarrow.parquet
from collections import defaultdict


def get_fname(adate, start_time, end_time, stripe):
    """ start_time is like 181500, end_time is like 182000, stripe is like AU-Bz """
    # have to prepend file:/dbfs
    return 'file:/dbfs/FileStore/tables/quotes_UsConsolidated_UsListing_%s_%s_%d_%d_txt.gz' % (stripe.replace('-', '_'), adate.replace('-', '_'), start_time, end_time)
  
def get_parquet_fname(adate, stripe):
    # have to prepend /dbfs
    return '/dbfs/tmp/parquet_data/quotes_UsConsolidated_UsListing_%s_%s.parquet' % (stripe.replace('-', '_'), adate.replace('-', '_'))

def get_atbest_fname():
    # have to prepend /dbfs
    return '/dbfs/FileStore/tables/atbest.csv'

def parse_one_row(row, nested_map):
    splits = row.split('|')
    symbol = splits[8]
    ticker, venue = symbol.split('.')
    amap = nested_map[symbol]
    for i in splits[10:]:
        k_v = i.split('=')
        if len(k_v) == 2:
            k, v = k_v
            if k in {'0', '1', '3', '5', '6', '8', '11'}:
                amap[k] = v
    atime = amap['3']
    if atime is None:
        atime = amap['8']
    return [ticker, amap['11'], atime, venue, amap['0'], amap['1'], amap['5'], amap['6']]

def encode_one_file(gzfname, nested_map):
    rawstrings = pd.read_csv(gzfname, compression='gzip', header=None, sep=',', quotechar='"')
    quotes = rawstrings[0].apply(lambda x: parse_one_row(x, nested_map)).apply(pd.Series)
    quotes.rename(columns={0: 'ticker', 1: 'date', 2: 'time', 3: 'venue', 4: 'bid', 5: 'bid_size', 6: 'ask', 7: 'ask_size'}, inplace=True)
    return quotes

def encode_one_day_one_stripe(adate, stripe):
    """ stripes is like AU-Bz
        this is the minimum logic unit that can be parallelized
    """
    times = [(181000, 181500), (181500, 182000)]  # this is only a demonstration
    list_quotes = []
    nested_map = defaultdict(dict)
    for start_time, end_time in times:
        fname = get_fname(adate, start_time, end_time, stripe)
        print('working on', fname)
        quotes = encode_one_file(fname, nested_map)
        list_quotes.append(quotes)
    day_quotes = pd.concat(list_quotes)
    return day_quotes

def write_parquet_one_day_one_stripe(date_stripe):
    adate, stripe = date_stripe
    day_quotes = encode_one_day_one_stripe(adate, stripe)
    atable = pyarrow.Table.from_pandas(day_quotes)
    parquet_fname = get_parquet_fname(adate, stripe)
    print('output parquet file', parquet_fname)
    pyarrow.parquet.write_table(atable, parquet_fname)
    return 1


dates = ['2017-12-11', ]
stripes = ['AU-Bz', 'CU-Dz',]
dates_stripes = [(x, y) for x in dates for y in stripes]
    
sc.parallelize(dates_stripes).map(write_parquet_one_day_one_stripe).collect()

# COMMAND ----------

dbutils.fs.ls('file:/dbfs/FileStore/tables')

# COMMAND ----------

dbutils.fs.ls('file:/dbfs/FileStore/tables/parquet_data')

# COMMAND ----------

data = sqlContext.read.parquet('file:/dbfs/FileStore/tables/parquet_data/')
data = data.filter((data.time >= '09:31:00.000') & (data.time <= '16:00:00.000'))
data[data['ticker']=='BAC'].head(3)

# COMMAND ----------

df = data.withColumn('minute', data['time'][0:5])
apanda = df.toPandas()

# COMMAND ----------

df.head(3)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType
from pyspark.sql.types import *
from pyspark.sql.functions import desc

# COMMAND ----------

def countDummy(agroup): 
    return len(agroup)
  
def countVenuesAtBest(agroup):
    last_quotes = agroup.sort_values(['time']).groupby('venue').last()
    try:
      last_nbbo = last_quotes.loc[''] # could have done better by testing the existence of consolidated quote 
      nbbo_bid = last_nbbo.bid
      nbbo_ask = last_nbbo.ask
      atbest = last_quotes[last_quotes['bid'] == nbbo_bid]
      count_atbest = len(atbest) - 1
    except:
      count_atbest = 0
    return count_atbest

schema = StructType([
    StructField("date", StringType()),
    StructField("minute", StringType()),
    StructField("ticker", StringType()),
    StructField("count", IntegerType())
])

@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def myagg(pdf):
    result = pd.DataFrame(pdf.groupby(['date', 'minute', 'ticker']).apply(
        lambda x: countVenuesAtBest(x)
    ))
    result.reset_index(inplace=True, drop=False)
    return result
  
myagg.func(apanda) # test input signature

# COMMAND ----------

atbest = df.groupBy('date', 'minute', 'ticker').apply(myagg)
atbest_pandas = atbest.toPandas()

# COMMAND ----------

atbest_fname = get_atbest_fname()
atbest_pandas.to_csv(atbest_fname, index=False)
