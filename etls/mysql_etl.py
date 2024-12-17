import mysql.connector
from mysql.connector import Error

def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host='mysql',      # Địa chỉ của MySQL (localhost vì bạn đã ánh xạ qua cổng 3307)
            port=3306,         # Sử dụng cổng 3307 để kết nối tới MySQL
            database='airflow_db', # Tên database bạn muốn kết nối
            user='airflow_user',   # Tên người dùng
            password='airflow_password' # Mật khẩu của người dùng
        )
        if connection.is_connected():
            print("Connected to MySQL database")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def create_table_if_not_exist(connection, table_name='reddit_data'):
    try:
        cursor = connection.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id VARCHAR(10) PRIMARY KEY,
            title TEXT,
            score INT,
            num_comments INT,
            author VARCHAR(255),
            created_utc TIMESTAMP,
            url TEXT,
            over_18 BOOLEAN,
            edited BOOLEAN,
            spoiler BOOLEAN,
            stickied BOOLEAN
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        print(f"Table '{table_name}' checked/created successfully")
    except Error as e:
        print(f"Error creating table in MySQL: {e}")

def upload_to_mysql(connection, data, table_name='reddit_data'):
    if not connection or not connection.is_connected():
        print("No valid connection to MySQL")
        return
    
    # Chuyển đổi giá trị boolean True/False thành 1/0
    transformed_data = [
        (
            item[0],             # id
            item[1],             # title
            item[2],             # score
            item[3],             # num_comments
            item[4],             # author
            item[5],             # created_utc
            item[6],             # url
            1 if item[7] else 0, # over_18 (chuyển đổi True/False sang 1/0)
            1 if item[8] else 0, # edited (chuyển đổi True/False sang 1/0)
            1 if item[9] else 0, # spoiler (chuyển đổi True/False sang 1/0)
            1 if item[10] else 0 # stickied (chuyển đổi True/False sang 1/0)
        )
        for item in data
    ]

    try:
        cursor = connection.cursor()
        insert_query = f"""
        INSERT INTO {table_name} 
        (id, title, score, num_comments, author, created_utc, url, over_18, edited, spoiler, stickied)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, transformed_data)
        connection.commit()
        print(f"Successfully inserted {cursor.rowcount} rows into table '{table_name}'")
        cursor.close()
    except Error as e:
        print(f"Error uploading data to MySQL: {e}")
