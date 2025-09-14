import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name = "product_enr_trns"
)
def append_sales_trns():
    df = spark.readStream.table("product_stg")
    df = df.withColumn("price",col("price").cast(IntegerType()))
    return df


dlt.create_streaming_table(
    name = "products_enr"
)
dlt.create_auto_cdc_flow(
    target = "products_enr",
    source = "product_enr_trns",
    keys = ["product_id"],
    sequence_by = "last_updated",
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None,
   
)