import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------------------------------------------------------
# bookings
# -----------------------------------------------------------------------

@dlt.table(
    name = 'bookings_stg'
)

def bookings_stg():
    df = spark.readStream.format('delta')\
        .load('/Volumes/workspace/bronze/volume/bookings/data')

    return df

@dlt.view(
    name = 'bookings_transformation'
)

def bookings_transformation():
    df = spark.readStream.table('bookings_stg')
    df = df.withColumn('amount', col('amount').cast(DoubleType()))\
           .withColumn('inserted_date', current_timestamp())\
           .withColumn('booking_date', to_date('booking_date'))\
           .drop('_rescued_data')

    return df

rules = {
    'rule1': 'booking_id is not null',
    'rule2': 'passenger_id is not null'
}

@dlt.table(
    name='silver_bookings'
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table('bookings_transformation')
    return df


# -----------------------------------------------------------------------
# flights
# -----------------------------------------------------------------------
@dlt.view(
    name = 'flights_transformation'
)
def flights_transformation():
    df = spark.readStream.format('delta')\
        .load('/Volumes/workspace/bronze/volume/flights/data')

    return df

dlt.create_streaming_table('silver_flights')

dlt.create_auto_cdc_flow(
    source='flights_transformation',
    target='silver_flights',
    keys = ['flight_id'],
    sequence_by = col('flight_id'),
    stored_as_scd_type=1
)

# -----------------------------------------------------------------------
# passengers
# -----------------------------------------------------------------------
@dlt.view(
    name = 'passengers_transformation'
)
def passengers_transformation():
    df = spark.readStream.format('delta')\
        .load('/Volumes/workspace/bronze/volume/customers/data')

    return df

dlt.create_streaming_table('silver_passengers')

dlt.create_auto_cdc_flow(
    source='passengers_transformation',
    target='silver_passengers',
    keys = ['passenger_id'],
    sequence_by = col('passenger_id'),
    stored_as_scd_type=1
)

# -----------------------------------------------------------------------
# airports
# -----------------------------------------------------------------------
@dlt.view(
    name = 'airports_transformation'
)
def airports_transformation():
    df = spark.readStream.format('delta')\
        .load('/Volumes/workspace/bronze/volume/airports/data')

    return df

dlt.create_streaming_table('silver_airports')

dlt.create_auto_cdc_flow(
    source='airports_transformation',
    target='silver_airports',
    keys = ['airport_id'],
    sequence_by = col('airport_id'),
    stored_as_scd_type=1
)
    