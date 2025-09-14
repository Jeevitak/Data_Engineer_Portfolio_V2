import dlt
customers_rules = {
    "rule_1":"customer_id IS NOT NULL",
    "rule_2":"customer_name IS NOT NULL"
    
}
@dlt.table(
    name = "customer_stg"
)
@dlt.expect_all_or_drop(customers_rules)

def cstomer_stg():
    df = spark.readStream.table("jeevita_dlt.source.customers")
    return df
