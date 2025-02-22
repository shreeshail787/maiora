import pandas as pd
import mysql.connector
import json

DB_CONFIG = {
    "host": "localhost",
    "user": "root",  
    "password": "admin",  
    "database": "shreeshail",  
}

def connect_db():
    return mysql.connector.connect(**DB_CONFIG)


def create_table():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_data (
            OrderId VARCHAR(255) PRIMARY KEY,
            OrderItemId VARCHAR(255),
            QuantityOrdered INT,
            ItemPrice FLOAT,
            PromotionDiscount FLOAT,
            total_sales FLOAT,
            net_sale FLOAT,
            region VARCHAR(50)
        )
    """)
    conn.commit()
    conn.close()


def extract_data(file_path):
    return pd.read_csv(file_path)

def transform_data(df, region):
    df["region"] = region  

    
    df.dropna(inplace=True)


    df["QuantityOrdered"] = df["QuantityOrdered"].astype(int)
    df["ItemPrice"] = df["ItemPrice"].astype(float)


    def extract_amount(value):
        try:
            if isinstance(value, str) and value.startswith("{"):
                value = json.loads(value)
                return float(value["Amount"])
            return float(value)
        except (ValueError, KeyError, json.JSONDecodeError):
            return 0.0  

    
    df["PromotionDiscount"] = df["PromotionDiscount"].apply(extract_amount)

    
    df["total_sales"] = df["QuantityOrdered"] * df["ItemPrice"]
    df["net_sale"] = df["total_sales"] - df["PromotionDiscount"]

    
    df.drop_duplicates(subset=["OrderId"], keep="first", inplace=True)

    df = df[df["net_sale"] > 0]

    return df


def load_data_to_db(df):
    conn = connect_db()
    cursor = conn.cursor()

    df.fillna({
        "OrderId": "", "OrderItemId": "", "QuantityOrdered": 0, 
        "ItemPrice": 0.0, "PromotionDiscount": 0.0, "total_sales": 0.0, 
        "net_sale": 0.0, "region": ""
    }, inplace=True)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO sales_data (OrderId, OrderItemId, QuantityOrdered, ItemPrice, PromotionDiscount, total_sales, net_sale, region)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                QuantityOrdered=VALUES(QuantityOrdered),
                ItemPrice=VALUES(ItemPrice),
                PromotionDiscount=VALUES(PromotionDiscount),
                total_sales=VALUES(total_sales),
                net_sale=VALUES(net_sale),
                region=VALUES(region)
        """, (
            row["OrderId"], row["OrderItemId"], row["QuantityOrdered"],
            row["ItemPrice"], row["PromotionDiscount"], row["total_sales"],
            row["net_sale"], row["region"]
        ))

    conn.commit()
    conn.close()

def validate_data():
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM sales_data")
    print("Total records:", cursor.fetchone()[0])

    cursor.execute("SELECT region, SUM(total_sales) FROM sales_data GROUP BY region")
    print("Total Sales by Region:")
    for row in cursor.fetchall():
        print(row)

    cursor.execute("SELECT COUNT(*) FROM sales_data WHERE net_sale <= 0")
    print("Invalid sales entries (should be 0):", cursor.fetchone()[0])

    conn.close()


if __name__ == "__main__":
    
    file1 = "C:/Users/acer/3D Objects/Downloads/order_region_a.csv"
    file2 = "C:/Users/acer/3D Objects/Downloads/order_region_b.csv"

    df1 = extract_data(file1)
    df2 = extract_data(file2)

    
    df1 = transform_data(df1, "A")
    df2 = transform_data(df2, "B")

    
    final_df = pd.concat([df1, df2])

    create_table()

    load_data_to_db(final_df)

    validate_data()
