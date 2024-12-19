import os
import sys
import pandas as pd
import random
import numpy as np
import mysql.connector
from mysql.connector import Error



def create_school_major_mapping():
    """
    Tạo từ điển ánh xạ logic giữa trường và ngành
    """
    return {
        "ĐẠI HỌC BÁCH KHOA HÀ NỘI": {
            "mã trường": "BKA",
            "ngành": {
                "Kỹ thuật Sinh học": ["BF1"],
                "Kỹ thuật Thực phẩm": ["BF2"],
                "Kỹ thuật Sinh học (CT tiên tiến)": ["BF-E19"],
                "Kỹ thuật Thực phẩm (CT tiên tiến)": ["BF-E12"],
                "Kỹ thuật Hóa dược (CT tiên tiến)": ["CH-E11"],
                "Kỹ thuật Hóa học": ["CH1"],
                "Hóa học": ["CH2"],
                "Công nghệ giáo dục": ["ED2"],
                "Hệ thống điện và năng lượng tái tạo (CT tiên tiến)": ["EE-E18"],
                "Kỹ thuật Điều khiển - Tự động hóa (CT tiên tiến)": ["EE-E8"],
                "Tin học công nghiệp và Tự động hóa (CT Việt-Pháp PFIEV)": ["EE-EP"],
                "Kỹ thuật điện": ["EE1"],
                "Kỹ thuật Điều khiển - Tự động hóa": ["EE2"],
                "Phân tích kinh doanh (CT tiên tiến)": ["EM-E13"],
                "Logistics và Quản lý chuỗi cung ứng (CT tiên tiến)": ["EM-E14"],
                "Quản lý năng lượng": ["EM1"],
                "Quản lý công nghiệp": ["EM2"],
                "Quản trị kinh doanh": ["EM3"],
                "Kế toán": ["EM4"],
                "Tài chính - Ngân hàng": ["EM5"],
                "Truyền thông số và Kỹ thuật đa phương tiện (CT tiên tiến)": ["ET-E16"],
                "Kỹ thuật Điện tử - Viễn thông (CT tiên tiến)": ["ET-E4"],
                "Kỹ thuật Y sinh (CT tiên tiến)": ["ET-E5"],
                "Hệ thống nhúng thông minh và IoT (CT tiên tiến)": ["ET-E9"],
                "Điện tử-Viễn thông - ĐH Leibniz Hannover (Đức)": ["ET-LUH"],
                "Điện tử và Viễn thông": ["ET1"],
                "Kỹ thuật Y sinh": ["ET2"],
                "Kỹ thuật Môi trường": ["EV1"],
                "Quản lý Tài nguyên và Môi trường": ["EV2"],
                "Tiếng Anh Khoa học Kỹ thuật và Công nghệ": ["FL1"],
                "Tiếng Anh chuyên nghiệp quốc tế": ["FL2"],
                "Kỹ thuật Nhiệt": ["HE1"],
                "Khoa học Dữ liệu và Trí tuệ Nhân tạo (CT tiên tiến)": ["IT-E10"],
                "An toàn không gian số (CT tiên tiến)": ["IT-E15"],
                "Công nghệ thông tin (Việt-Nhật) (CT tiên tiến)": ["IT-E6"],
                "Công nghệ thông tin (Global ICT)": ["IT-E7"],
                "Công nghệ thông tin (Việt-Pháp) (CT tiên tiến)": ["IT-EP"],
                "CNTT: Khoa học Máy tính": ["IT1"],
                "CNTT: Kỹ thuật máy tính": ["IT2"],
                "Kỹ thuật Cơ điện tử (CT tiên tiến)": ["ME-E1"],
                "Cơ khí - Chế tạo máy - ĐH Griffith (Úc)": ["ME-GU"],
                "Cơ điện tử - ĐH Leibniz Hannover (Đức)": ["ME-LUH"],
                "Cơ điện tử - ĐH Nagaoka (Nhật Bản)": ["ME-NUT"],
                "Kỹ thuật Cơ điện tử": ["ME1"],
                "Kỹ thuật Cơ khí": ["ME2"],
                "Toán - Tin": ["MI1"],
                "Hệ thống thông tin quản lý": ["MI2"],
                "Khoa học và Kỹ thuật Vật liệu (CT tiên tiến)": ["MS-E3"],
                "Kỹ thuật Vật liệu": ["MS1"],
                "Chương trình Kỹ thuật vi điện tử và công nghệ Nano": ["MS2"],
                "Công nghệ vật liệu polyme và compozit": ["MS3"],
                "Kỹ thuật in": ["MS5"],
                "Vật lý kỹ thuật": ["PH1"],
                "Kỹ thuật hạt nhân": ["PH2"],
                "Vật lý Y khoa": ["PH3"],
                "Kỹ thuật Ô tô (CT tiên tiến)": ["TE-E2"],
                "Cơ khí hàng không (CT Việt - Pháp PFIEV)": ["TE-EP"],
                "Kỹ thuật Ô tô": ["TE1"],
                "Kỹ thuật Cơ khí động lực": ["TE2"],
                "Kỹ thuật Hàng không": ["TE3"],
                "Quản trị kinh doanh - ĐH Troy (Hoa Kỳ)": ["TROY-BA"],
                "Khoa học máy tính - ĐH Troy (Hoa Kỳ)": ["TROY-IT"],
                "Công nghệ Dệt May": ["TX1"]
            }
        },
        "TRƯỜNG ĐẠI HỌC KHOA HỌC TỰ NHIÊN - ĐH QG HÀ NỘI": {
            "mã trường": "QHT",
            "ngành": {
                "Kỹ thuật Sinh học": ["BF1", "BF2", "BF-E12", "BF-E19"],
                "Khoa học Môi trường": ["EM3", "EM4", "EM5"]
            }
        },
        "HỌC VIỆN CÔNG NGHỆ BƯU CHÍNH VIỄN THÔNG": {
            "mã trường": "BVH",
            "ngành": {
                "Kỹ thuật Viễn thông": ["QHT94"],
                "Công nghệ Thông tin": ["TROY-IT"]
            }
        },
        # Các trường khác...
    }

def get_random_major_for_school(school_mapping, ten_truong):
    """
    Lấy ngành học ngẫu nhiên phù hợp với trường
    """
    school_data = school_mapping.get(ten_truong, {})
    if not school_data.get("ngành"):
        return None, None
    
    ten_nganh = random.choice(list(school_data["ngành"].keys()))
    ma_nganh = random.choice(school_data["ngành"][ten_nganh])
    
    return ten_nganh, ma_nganh

def process_wish_data(stage, **kwargs):
    """
    Xử lý sinh dữ liệu nguyện vọng theo từng giai đoạn
    
    Args:
        stage (int): Giai đoạn xử lý dữ liệu (1 hoặc 2)
    """
    # # Danh sách các ngành học và trường
    # majors = ["EE-E18", "QHT94", "IT-E7", "EM3", "BF2", "CH2","BF1","BF2","BF-E12","BF-E19","CH1","CH2","CH3","CH-E11","ED2","EE1","EE2","EE-E18","EE-E8","EE-EP","EM1","EM3","EM4","EM5","EM-E13","EM-E14","EM-VUW","ET1","ET2","TROY-BA","TROY-IT","TX1"]
    # schools = ["ĐẠI HỌC BÁCH KHOA HÀ NỘI", "TRƯỜNG ĐẠI HỌC KHOA HỌC TỰ NHIÊN - ĐH QG HÀ NỘI","HỌC VIỆN CÔNG NGHỆ BƯU CHÍNH VIỄN THÔNG","TRƯỜNG ĐẠI HỌC CÔNG NGHIỆP HÀ NỘI","TRƯỜNG ĐẠI HỌC CMC","TRƯỜNG ĐẠI HỌC KINH TẾ KỸ THUẬT CÔNG NGHIỆP","TRƯỜNG ĐẠI HỌC CÔNG NGHIỆP HÀ NỘI"]
    
    # Thiết lập thư mục lưu trữ dữ liệu
    data_dir = '/opt/airflow/data/wish_data'
    os.makedirs(data_dir, exist_ok=True)
    
    if stage == 1:
        # Giai đoạn 1: Sinh dữ liệu sinh viên và chứng chỉ
        num_students = 20000
        
        # 1. Tạo danh sách ID sinh viên (id_sv) duy nhất
        id_sv_list = [f"{random.randint(10000000000, 99999999999)}" for _ in range(num_students)]

        # 2. Tạo bảng `sinh_vien`
        sinh_vien = pd.DataFrame({
            'id_sv': id_sv_list,
            'ten_sv': [f"Họ Tên {i}" for i in range(num_students)],
            'ngay_sinh': pd.to_datetime(np.random.choice(pd.date_range('2002-01-01', '2006-12-31'), num_students)),
            'gioi_tinh': np.random.choice(['Nam', 'Nữ'], num_students),
            'uu_tien': np.random.choice(['Không', 'Đối tượng 1', 'Đối tượng 2'], num_students),
            'khu_vuc': np.random.choice(['KV1', 'KV2', 'KV3'], num_students),
            'diem_khuyen_khich': np.random.uniform(0, 2, num_students).round(2),
            'ma_tinh': [id_sv[:3] for id_sv in id_sv_list],
            'loai_thi_sinh': np.random.choice(['Thí sinh không phải tự do', 'Thí sinh tự do'], num_students)
        })

        # 3. Tạo bảng `cc_sinh_vien`
        cc_sinh_vien = pd.DataFrame({
            'id_sv': id_sv_list,
            'id_cc': [f"CC{i}" for i in range(num_students)],
            'ngoai_ngu': np.random.choice(['Tiếng Anh', 'Tiếng Nhật', 'Tiếng Pháp'], num_students),
            'code_ngoai_ngu': np.random.choice(['N1', 'N2', 'N3'], num_students),
            'loai_cc': np.random.choice(['IELTS', 'TOEFL', 'JLPT'], num_students),
            'diem_cc': np.random.uniform(4.0, 9.0, num_students).round(1),
            'diem_quy_doi_THPT': np.random.uniform(8.0, 10.0, num_students).round(1)
        })

        # Lưu các bảng
        sinh_vien.to_csv(os.path.join(data_dir, 'sinh_vien.csv'), index=False, encoding='utf-8-sig')
        cc_sinh_vien.to_csv(os.path.join(data_dir, 'cc_sinh_vien.csv'), index=False, encoding='utf-8-sig')
        
        print(f"Sinh dữ liệu giai đoạn 1 thành công. Tổng: {num_students} sinh viên")
   
        sinh_vien_df = pd.read_csv(os.path.join(data_dir, 'sinh_vien.csv'))
        id_sv_list = sinh_vien_df['id_sv'].tolist()
        
        # 4. Tạo bảng `nguyen_vong`
        school_mapping = create_school_major_mapping()
        nguyen_vong_list = []
        for id_sv in id_sv_list:
            # Mỗi sinh viên có từ 1 đến 5 nguyện vọng
            num_nv = random.randint(1, 5)
            for nv in range(1, num_nv + 1):
                # Chọn ngẫu nhiên một trường có trong mapping
                ten_truong = random.choice(list(school_mapping.keys()))
                ma_truong = school_mapping[ten_truong]["mã trường"]
                
                # Lấy ngành phù hợp với trường
                ten_nganh, ma_nganh = get_random_major_for_school(school_mapping, ten_truong)
                nguyen_vong_list.append({
                    'id_sv': id_sv,
                    'id_nv': f"{id_sv}_NV{nv}",  # Định danh nguyện vọng dựa trên id_sv
                    'tt_nv': nv,  # Thứ tự nguyện vọng
                    'ma_truong': ma_truong,
                    'ten_truong': ten_truong,
                    'ma_nganh': ma_nganh,
                    'ten_nganh': ten_nganh,
                    'phuong_thuc_1': np.random.choice([1, 0]),
                    'phuong_thuc_2': np.random.choice([1, 0]),
                    'phuong_thuc_3': np.random.choice([1, 0]),
                    'phuong_thuc_4': np.random.choice([1, 0])
                })

        nguyen_vong = pd.DataFrame(nguyen_vong_list)

        # 5. Tạo bảng `ket_qua_xet_tuyen`
        ket_qua_xet_tuyen = pd.DataFrame({
            'id_sv': id_sv_list,
            'tong_diem_xet_tuyen': np.random.uniform(20.0, 30.0, len(id_sv_list)).round(2),
            'nganh_tt': np.random.choice(['CH2', 'EM3', 'ET1', 'IT-E7', 'BF2'], len(id_sv_list))
        })

        # Lưu các bảng
        nguyen_vong.to_csv(os.path.join(data_dir, 'nguyen_vong.csv'), index=False, encoding='utf-8-sig')
        ket_qua_xet_tuyen.to_csv(os.path.join(data_dir, 'ket_qua_xet_tuyen.csv'), index=False, encoding='utf-8-sig')
        
        print(f"Sinh dữ liệu giai đoạn 2 thành công. Tổng: {len(id_sv_list)} sinh viên")
        
    elif stage == 2:
        
        
        print(f"Sinh dữ liệu giai đoạn 2 thành công. Tổng: {len(id_sv_list)} sinh viên")
    
    else:
        raise ValueError("Stage phải là 1 hoặc 2")


def etl_to_mysql():
    # Đường dẫn thư mục chứa các file CSV
    data_dir = '/opt/airflow/data/wish_data'
    
    # Kết nối MySQL
    try:
        connection = mysql.connector.connect(
            host='mysql',      
            port=3306,         
            database='airflow_db', 
            user='airflow_user',   
            password='airflow_password'
        )
        
        if not connection.is_connected():
            print("Không thể kết nối đến MySQL")
            return
        
        cursor = connection.cursor()
        
        # Đọc dữ liệu từ các file CSV
        try:
            sinh_vien = pd.read_csv(os.path.join(data_dir, 'sinh_vien.csv'), encoding='utf-8-sig')
            nguyen_vong = pd.read_csv(os.path.join(data_dir, 'nguyen_vong.csv'), encoding='utf-8-sig')
            ket_qua_xet_tuyen = pd.read_csv(os.path.join(data_dir, 'ket_qua_xet_tuyen.csv'), encoding='utf-8-sig')
            cc_sinh_vien = pd.read_csv(os.path.join(data_dir, 'cc_sinh_vien.csv'), encoding='utf-8-sig')
        except FileNotFoundError as e:
            print(f"Lỗi: Không tìm thấy file dữ liệu - {e}")
            return

        # Xử lý dữ liệu
        sinh_vien['ngay_sinh'] = pd.to_datetime(sinh_vien['ngay_sinh'], errors='coerce')
        nguyen_vong = nguyen_vong.dropna(subset=['id_sv', 'ma_nganh'])
        cc_sinh_vien = cc_sinh_vien.dropna(subset=['id_sv', 'loai_cc'])
        
        sinh_vien['gioi_tinh'] = sinh_vien['gioi_tinh'].apply(lambda x: 'Nam' if x in ['Nam', 'male'] else 'Nữ')
        sinh_vien['khu_vuc'] = sinh_vien['khu_vuc'].apply(lambda x: 'KV1' if 'KV1' in str(x) else ('KV2' if 'KV2' in str(x) else 'KV3'))
        sinh_vien['diem_khuyen_khich'] = sinh_vien['diem_khuyen_khich'].fillna(0)
        sinh_vien['gioi_tinh'] = sinh_vien['gioi_tinh'].fillna('Không xác định')
        sinh_vien['khu_vuc'] = sinh_vien['khu_vuc'].fillna('Không xác định')
        
        sinh_vien = sinh_vien.drop_duplicates(subset=['id_sv'])
        nguyen_vong = nguyen_vong.drop_duplicates(subset=['id_sv', 'id_nv'])

        # Tạo các bảng nếu chưa tồn tại
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sinh_vien (
            id_sv VARCHAR(20) PRIMARY KEY,
            ten_sv VARCHAR(255),
            ngay_sinh DATE,
            gioi_tinh VARCHAR(20),
            uu_tien VARCHAR(50),
            khu_vuc VARCHAR(10),
            diem_khuyen_khich FLOAT,
            ma_tinh VARCHAR(10),
            loai_thi_sinh VARCHAR(50)
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS nguyen_vong (
            id_sv VARCHAR(20),
            id_nv VARCHAR(50),
            tt_nv INT,
            ma_truong VARCHAR(20),
            ten_truong VARCHAR(255),
            ma_nganh VARCHAR(20),
            ten_nganh VARCHAR(255),
            phuong_thuc_1 INT,
            phuong_thuc_2 INT,
            phuong_thuc_3 INT,
            phuong_thuc_4 INT,
            PRIMARY KEY (id_sv, id_nv)
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ket_qua_xet_tuyen (
            id_sv VARCHAR(20) PRIMARY KEY,
            tong_diem_xet_tuyen FLOAT,
            nganh_tt VARCHAR(20)
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cc_sinh_vien (
            id_sv VARCHAR(20),
            id_cc VARCHAR(50),
            ngoai_ngu VARCHAR(50),
            code_ngoai_ngu VARCHAR(10),
            loai_cc VARCHAR(50),
            diem_cc FLOAT,
            diem_quy_doi_THPT FLOAT,
            PRIMARY KEY (id_sv, id_cc)
        )
        """)

        # Xóa dữ liệu cũ
        cursor.execute("DELETE FROM sinh_vien")
        cursor.execute("DELETE FROM nguyen_vong")
        cursor.execute("DELETE FROM ket_qua_xet_tuyen")
        cursor.execute("DELETE FROM cc_sinh_vien")

        # Chèn dữ liệu mới
        def insert_dataframe(cursor, table_name, df):
            placeholders = ', '.join(['%s'] * len(df.columns))
            columns = ', '.join(df.columns)
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Chuyển đổi dữ liệu DataFrame thành danh sách các tuple
            records = [tuple(row) for row in df.to_numpy()]
            
            cursor.executemany(insert_query, records)

        # Thực hiện chèn dữ liệu
        insert_dataframe(cursor, 'sinh_vien', sinh_vien)
        insert_dataframe(cursor, 'nguyen_vong', nguyen_vong)
        insert_dataframe(cursor, 'ket_qua_xet_tuyen', ket_qua_xet_tuyen)
        insert_dataframe(cursor, 'cc_sinh_vien', cc_sinh_vien)

        # Commit thay đổi
        connection.commit()
        print("Đã ghi dữ liệu thành công!")

    except Error as e:
        print(f"Lỗi khi thực hiện ETL: {e}")
    
    finally:
        # Đóng kết nối
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Kết nối MySQL đã đóng")