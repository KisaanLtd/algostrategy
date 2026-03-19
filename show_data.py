
import asyncio
import aiomysql
from dotenv import load_dotenv
import os
import ssl

async def main():
    load_dotenv()
    db_config = {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT")),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "db": os.getenv("DB_NAME"),
    }

    ssl_context = ssl.create_default_context(cafile='ca.pem')

    try:
        pool = await aiomysql.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['db'],
            autocommit=True,
            ssl=ssl_context
        )

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                print("--- Tables in database ---")
                await cursor.execute("SHOW TABLES;")
                tables = await cursor.fetchall()
                for table in tables:
                    print(table[0])

                print("\n--- Data from ohlctick_1mdata (first 10 rows) ---")
                await cursor.execute("SELECT * FROM ohlctick_1mdata LIMIT 10;")
                rows = await cursor.fetchall()
                for row in rows:
                    print(row)

        pool.close()
        await pool.wait_closed()

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())
