# crypto_snowflake.py
import snowflake.connector

def insert_into_snowflake(bitcoin_usd, ethereum_usd):
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user='JEEVITAK',
        password='Imdnwtevrythng@745',
        account='GLPLNTQ-YMB56622',       
        warehouse='COMPUTE_WH',
        database='CRYPTO_DB',
        schema='STAGING'
    )
    cursor = conn.cursor()

    # Insert the data into crypto_prices table
    cursor.execute("""
        INSERT INTO crypto_prices (bitcoin_usd, ethereum_usd)
        VALUES (%s, %s)
    """, (bitcoin_usd, ethereum_usd))

    cursor.close()
    conn.close()