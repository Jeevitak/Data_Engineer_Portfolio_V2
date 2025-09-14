import dlt

products_rules={
     "rule_1" : "product_id is NOT NULL",
     "rule_2" : "price >= 0 "
}
@dlt.table(
    name = "product_stg"
)
@dlt.expect_all_or_drop(products_rules)
def product_stg():
    df = spark.readStream.table("jeevita_dlt.source.products")
    return df
