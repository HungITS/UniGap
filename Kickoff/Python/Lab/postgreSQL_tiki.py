import pandas as pd
import psycopg2 
from config import load_config

def create_tables():
    sql = ("""CREATE TABLE products (
                product_id BIGINT PRIMARY KEY,
                name TEXT,
                url_key TEXT,
                price NUMERIC,
                description TEXT)""",
                
            """CREATE TABLE images (
                id SERIAL PRIMARY KEY,
                product_id BIGINT,
                base_url TEXT,
                large_url TEXT,
                medium_url TEXT,
                small_url TEXT,
                thumbnail_url TEXT,
                FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE)""")
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for s in sql:
                    cur.execute(s)
        print("Create table successfully")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def insert_data(df, table_name):
    
    columns = list(df.columns)
    columns_str = ", ".join(columns)
    
    values = ", ".join(["%s"] * len(columns))

    sql = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({values});
    """

    data = df.to_records(index=False).tolist()
    config = load_config()

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, data)
        conn.commit()
    except Exception as error:
        print(error)
        conn.rollback()
    
    
if __name__ == "__main__":
    create_tables()

    file_path_temp = '/home/hungits/Documents/UniGap/Kickoff/Python/Lab/json_file/products_batch_{0}.json'
    
    products = pd.DataFrame()
    images = pd.DataFrame()
    
    for i in range(1, 201):
        file = file_path_temp.format(i)
        print("Đang đọc file products_batch_{0}.json".format(i))

        try:
            df = pd.read_json(file)

            
            product_data = df[['id', 'name', 'url_key', 'price', 'description']]
            product_data = product_data.rename(columns={'id': 'product_id'})

            image_data = df.explode('images')

            image_data = pd.json_normalize(image_data['images'])
            image_data['product_id'] = df['id'].iloc[0]

            columns = ['product_id'] + [col for col in image_data.columns if col != 'product_id']
            image_data = image_data[columns]

            insert_data(product_data, 'products')
            insert_data(image_data, 'images')

        except Exception as error:
            print("Lỗi khi xử lý file {file}: {error}")
