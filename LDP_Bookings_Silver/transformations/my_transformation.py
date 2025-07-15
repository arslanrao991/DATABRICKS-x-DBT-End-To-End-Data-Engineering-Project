import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt_table(
    name = 'bookings_stg'
)

def bookings_stg():
    df = spark.readStream.format('delta')\
        .load('/Volumns/workspace/bronze/volume/bookings/data')

    return df

@dtl_view(
    name = 'bookings_transformation'
)

def bookings_trans():
    df = spark.readStream.table('bookings_stg')
    df = df.withColumn('amount', col('amount').cast(DoubleType()))\
           .withColumn('inserted_date', current_timestamp())\
           .withColumn('booking_date', to_date('booking_date'))\
           .drop('resc')


