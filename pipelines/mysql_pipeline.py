from etls.mysql_etl import connect_to_mysql, create_table_if_not_exist, upload_to_mysql

def load_data_to_mysql(ti, table_name='reddit_data'):
    file_path = ti.xcom_pull(task_ids='reddit_extraction', key='return_value')
    
    # Đọc dữ liệu từ file_path, giả sử bạn đang lưu dữ liệu dưới dạng CSV
    data = []
    with open(file_path, 'r') as file:
        next(file)  # Bỏ qua dòng tiêu đề nếu có
        for line in file:
            fields = line.strip().split(',')
            # Giả sử cấu trúc của file là (title, score, created_utc, subreddit)
            data.append(tuple(fields))
    
    # Kết nối đến MySQL
    connection = connect_to_mysql()
    
    # Tạo bảng nếu chưa tồn tại
    create_table_if_not_exist(connection, table_name)
    
    # Upload dữ liệu vào MySQL
    upload_to_mysql(connection, data, table_name)
    
    # Đóng kết nối
    if connection.is_connected():
        connection.close()
        print("MySQL connection closed")
