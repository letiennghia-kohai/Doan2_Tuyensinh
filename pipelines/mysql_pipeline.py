import os
import sys
import mysql.connector
import psycopg2
from mysql.connector import Error
from psycopg2.extras import execute_values

def etl_to_olap():
    """
    ETL process to transform university_data database into olap_university_data star schema using PostgreSQL
    """
    try:
        # Establish connection to source database (MySQL)
        source_connection = mysql.connector.connect(
            host='mysql',      
            port=3306,         
            database='airflow_db', 
            user='airflow_user',   
            password='airflow_password'
        )
        
        # Establish connection to target OLAP database (PostgreSQL)
        olap_connection = psycopg2.connect(
            host='postgres',      
            port=5432,         
            database='olap_university_data', 
            user='postgres',   
            password='postgres'
        )
        
        # Create cursors
        source_cursor = source_connection.cursor(dictionary=True)
        olap_cursor = olap_connection.cursor()
        
        # 1. Extract đối tượng ưu tiên từ MySQL và transform thành dim table
        source_cursor.execute("SELECT DISTINCT uu_tien FROM sinh_vien WHERE uu_tien IS NOT NULL")
        dt_uu_tien_data = source_cursor.fetchall()
        
        olap_cursor.execute("""
        DROP TABLE IF EXISTS dim_dt_uu_tien CASCADE;
        CREATE TABLE dim_dt_uu_tien (
            id_dt_uu_tien SERIAL PRIMARY KEY,
            nhom_uu_tien VARCHAR(50) UNIQUE,
            kieu_uu_tien VARCHAR(50),
            diem_khuyen_khich FLOAT,
            mo_ta VARCHAR(255)
        )
        """)
        
        dt_uu_tien_insert = []
        for row in dt_uu_tien_data:
            uu_tien = row['uu_tien']
            diem_khuyen_khich = 2.0 if uu_tien == 'Đối tượng 1' else 1.0 if uu_tien == 'Đối tượng 2' else 0.0
            dt_uu_tien_insert.append((
                uu_tien, 
                'Đối tượng ưu tiên', 
                diem_khuyen_khich, 
                f'Nhóm ưu tiên {uu_tien}'
            ))
        
        execute_values(olap_cursor, """
        INSERT INTO dim_dt_uu_tien 
        (nhom_uu_tien, kieu_uu_tien, diem_khuyen_khich, mo_ta) 
        VALUES %s
        """, dt_uu_tien_insert)
        
        # 2. Extract khu vực ưu tiên từ MySQL và transform thành dim table
        source_cursor.execute("SELECT DISTINCT khu_vuc FROM sinh_vien WHERE khu_vuc IS NOT NULL")
        kv_uu_tien_data = source_cursor.fetchall()
        
        olap_cursor.execute("""
        DROP TABLE IF EXISTS dim_kv_uu_tien CASCADE;
        CREATE TABLE dim_kv_uu_tien (
            id_khu_vuc SERIAL PRIMARY KEY,
            kieu_khu_vuc VARCHAR(10) UNIQUE,
            diem_khuyen_khich FLOAT,
            mo_ta VARCHAR(255)
        )
        """)
        
        kv_uu_tien_insert = []
        for row in kv_uu_tien_data:
            khu_vuc = row['khu_vuc']
            diem_khuyen_khich = 2.0 if khu_vuc == 'KV1' else 1.0 if khu_vuc == 'KV2' else 0.0
            kv_uu_tien_insert.append((
                khu_vuc, 
                diem_khuyen_khich, 
                f'Khu vực ưu tiên {khu_vuc}'
            ))
        
        execute_values(olap_cursor, """
        INSERT INTO dim_kv_uu_tien 
        (kieu_khu_vuc, diem_khuyen_khich, mo_ta) 
        VALUES %s
        """, kv_uu_tien_insert)
        
        # 3. Extract tỉnh từ MySQL và transform thành dim table
        source_cursor.execute("SELECT DISTINCT ma_tinh FROM sinh_vien WHERE ma_tinh IS NOT NULL")
        tinh_data = source_cursor.fetchall()
        
        olap_cursor.execute("""
        DROP TABLE IF EXISTS dim_tinh CASCADE;
        CREATE TABLE dim_tinh (
            id_dim_tinh SERIAL PRIMARY KEY,
            ma_tinh VARCHAR(10) UNIQUE,
            khu_vuc VARCHAR(50),
            vung_mien VARCHAR(50)
        )
        """)
        
        tinh_insert = []
        for row in tinh_data:
            ma_tinh = row['ma_tinh']
            # Chuyển mã tỉnh về string để so sánh
            ma_tinh_str = str(ma_tinh)
            khu_vuc = 'Bắc' if ma_tinh_str <= '30' else 'Trung' if ma_tinh_str <= '50' else 'Nam'
            vung_mien = 'Miền Bắc' if ma_tinh_str <= '30' else 'Miền Trung' if ma_tinh_str <= '50' else 'Miền Nam'
            
            tinh_insert.append((
                ma_tinh, 
                khu_vuc, 
                vung_mien
            ))
        
        execute_values(olap_cursor, """
        INSERT INTO dim_tinh 
        (ma_tinh, khu_vuc, vung_mien) 
        VALUES %s
        """, tinh_insert)
        
        # 4. Extract trường từ MySQL và transform thành dim table
        source_cursor.execute("""
        SELECT DISTINCT 
            ma_truong, 
            MAX(ten_truong) as ten_truong, 
            ma_nganh, 
            MAX(ten_nganh) as ten_nganh 
        FROM nguyen_vong 
        WHERE ma_truong IS NOT NULL 
            AND ma_nganh IS NOT NULL
        GROUP BY ma_truong, ma_nganh
        """)
        truong_data = source_cursor.fetchall()
        
        olap_cursor.execute("""
        DROP TABLE IF EXISTS dim_truong CASCADE;
        CREATE TABLE dim_truong (
            id_dim_truong SERIAL PRIMARY KEY,
            ma_truong VARCHAR(20),
            ten_truong VARCHAR(255),
            ma_nganh VARCHAR(20),
            ten_nganh VARCHAR(255),
            chi_tieu INT DEFAULT 100,
            UNIQUE (ma_truong, ma_nganh)
        )
        """)
        
        truong_insert = []
        for row in truong_data:
            truong_insert.append((
                row['ma_truong'], 
                row['ten_truong'], 
                row['ma_nganh'], 
                row['ten_nganh']
            ))
        
        execute_values(olap_cursor, """
        INSERT INTO dim_truong 
        (ma_truong, ten_truong, ma_nganh, ten_nganh) 
        VALUES %s
        """, truong_insert)
        
        # 5. Extract sinh viên từ MySQL
        source_cursor.execute("""
        SELECT 
            sv.id_sv, 
            sv.ngay_sinh, 
            sv.gioi_tinh,
            sv.uu_tien,
            sv.khu_vuc,
            sv.diem_khuyen_khich,
            sv.ma_tinh,
            sv.loai_thi_sinh,
            MAX(sv.ten_sv) as ten_sv
        FROM sinh_vien sv
        GROUP BY 
            sv.id_sv, 
            sv.ngay_sinh, 
            sv.gioi_tinh,
            sv.uu_tien,
            sv.khu_vuc,
            sv.diem_khuyen_khich,
            sv.ma_tinh,
            sv.loai_thi_sinh
        """)
        sinh_vien_data = source_cursor.fetchall()
        
        # Get mappings from dimension tables
        olap_cursor.execute("SELECT nhom_uu_tien, id_dt_uu_tien FROM dim_dt_uu_tien")
        dt_uu_tien_mapping = {row[0]: row[1] for row in olap_cursor.fetchall()}
        
        olap_cursor.execute("SELECT kieu_khu_vuc, id_khu_vuc FROM dim_kv_uu_tien")
        kv_uu_tien_mapping = {row[0]: row[1] for row in olap_cursor.fetchall()}
        
        # Create dim_sinh_vien table
        olap_cursor.execute("""
        DROP TABLE IF EXISTS dim_sinh_vien CASCADE;
        CREATE TABLE dim_sinh_vien (
            id_dim_sv SERIAL PRIMARY KEY,
            id_sv VARCHAR(20) UNIQUE,
            ngay_sinh DATE,
            gioi_tinh VARCHAR(20),
            id_dt_uu_tien INT,
            id_khu_vuc INT,
            diem_khuyen_khich FLOAT,
            ma_tinh VARCHAR(10),
            loai_thi_sinh VARCHAR(50),
            ten_sv VARCHAR(255),
            FOREIGN KEY (id_dt_uu_tien) REFERENCES dim_dt_uu_tien(id_dt_uu_tien),
            FOREIGN KEY (id_khu_vuc) REFERENCES dim_kv_uu_tien(id_khu_vuc)
        )
        """)
        
        sinh_vien_insert = []
        for row in sinh_vien_data:
            sinh_vien_insert.append((
                row['id_sv'], 
                row['ngay_sinh'], 
                row['gioi_tinh'],
                dt_uu_tien_mapping.get(row['uu_tien']),
                kv_uu_tien_mapping.get(row['khu_vuc']),
                row['diem_khuyen_khich'],
                row['ma_tinh'],
                row['loai_thi_sinh'],
                row['ten_sv']
            ))
        
        execute_values(olap_cursor, """
        INSERT INTO dim_sinh_vien 
        (id_sv, ngay_sinh, gioi_tinh, id_dt_uu_tien, id_khu_vuc, 
         diem_khuyen_khich, ma_tinh, loai_thi_sinh, ten_sv) 
        VALUES %s
        """, sinh_vien_insert)
        
        # 6. Extract nguyện vọng và chứng chỉ từ MySQL
        source_cursor.execute("""
        SELECT 
            nv.id_sv,
            nv.tt_nv,
            nv.ma_truong,
            nv.ma_nganh,
            CASE 
                WHEN nv.phuong_thuc_1 = 1 THEN 1 
                WHEN nv.phuong_thuc_2 = 1 THEN 2 
                WHEN nv.phuong_thuc_3 = 1 THEN 3 
                WHEN nv.phuong_thuc_4 = 1 THEN 4 
                ELSE 0 
            END AS phuong_thuc,
            COALESCE(cc.ngoai_ngu, 'Không') AS ngoai_ngu,
            COALESCE(cc.code_ngoai_ngu, 'N/A') AS code_ngoai_ngu,
            COALESCE(cc.loai_cc, 'Không') AS loai_cc,
            COALESCE(cc.diem_cc, 0) AS diem_cc,
            COALESCE(cc.diem_quy_doi_THPT, 0) AS diem_quy_doi_THPT
        FROM nguyen_vong nv
        LEFT JOIN cc_sinh_vien cc ON nv.id_sv = cc.id_sv
        WHERE nv.id_sv IS NOT NULL
        """)
        nguyen_vong_data = source_cursor.fetchall()
        
        # Get mapping from dim_truong
        olap_cursor.execute("SELECT ma_truong, ma_nganh, id_dim_truong FROM dim_truong")
        truong_mapping = {(row[0], row[1]): row[2] for row in olap_cursor.fetchall()}
        
        # Create fact_nguyen_vong table
        olap_cursor.execute("""
        DROP TABLE IF EXISTS fact_nguyen_vong;
        CREATE TABLE fact_nguyen_vong (
            id_fact_nv SERIAL PRIMARY KEY,
            id_sv VARCHAR(20),
            tt_nv INT,
            ma_truong INT,
            phuong_thuc INT,
            ngoai_ngu VARCHAR(50),
            code_ngoai_ngu VARCHAR(10),
            loai_cc VARCHAR(50),
            diem_cc FLOAT,
            diem_quy_doi_THPT FLOAT,
            FOREIGN KEY (id_sv) REFERENCES dim_sinh_vien(id_sv)
        )
        """)
        
        nguyen_vong_insert = []
        for row in nguyen_vong_data:
            # Get correct truong_id from mapping
            truong_id = truong_mapping.get((row['ma_truong'], row['ma_nganh']))
            if truong_id:  # Only insert if we have a valid truong_id
                nguyen_vong_insert.append((
                    row['id_sv'],
                    row['tt_nv'],
                    truong_id,
                    row['phuong_thuc'],
                    row['ngoai_ngu'],
                    row['code_ngoai_ngu'],
                    row['loai_cc'],
                    row['diem_cc'],
                    row['diem_quy_doi_THPT']
                ))
        
        execute_values(olap_cursor, """
        INSERT INTO fact_nguyen_vong 
        (id_sv, tt_nv, ma_truong, phuong_thuc, 
         ngoai_ngu, code_ngoai_ngu, loai_cc, diem_cc, diem_quy_doi_THPT) 
        VALUES %s
        """, nguyen_vong_insert)
        
        # Commit changes
        olap_connection.commit()
        print("OLAP transformation completed successfully!")
        
    except (mysql.connector.Error, psycopg2.Error) as e:
        print(f"Error during OLAP transformation: {e}")
        if olap_connection:
            olap_connection.rollback()
    
    finally:
        # Close connections
        if source_connection.is_connected():
            source_cursor.close()
            source_connection.close()
        
        if olap_connection is not None:
            olap_cursor.close()
            olap_connection.close()