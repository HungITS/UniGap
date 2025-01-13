# main.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pyodbc
from config.config import load_config

def convert_null_to_none(value):
    """Chuyển các giá trị null (chuỗi 'null' hoặc giá trị None) thành None"""
    if value == 'null' or value is None:
        return None
    return value

def insert_from_file(file_name, table_name, batch_size):
    batch = []
    config = load_config()  # Gọi hàm load_config từ config.py

    # Chuỗi kết nối
    connection_string = f"DRIVER={{{config['driver']}}};" \
                     f"SERVER={config['server']};" \
                     f"DATABASE={config['database']};" \
                     f"UID={config['username']};" \
                     f"PWD={config['password']};" \
                     f"Encrypt=no;"


    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    script_dir = os.path.dirname(__file__)  # Thư mục chứa script này
    project_root = os.path.abspath(os.path.join(script_dir, '..'))  # Lên một cấp để vào thư mục gốc của dự án
    file_path = os.path.join(project_root, 'data', file_name)
    
    with open(file_path, 'r', encoding='utf-8') as file:
        record = ""
        for line in file:
            record += line.strip()
            # Loại bỏ dấu ',' cuối dòng nếu có
            if record.endswith(','):
                record = record[:-1]
            
            
            # Kiểm tra nếu dòng dữ liệu kết thúc bằng dấu ngoặc đóng ')'
            if record.endswith(")"):
                try:
                    # Đảm bảo mỗi bản ghi có cặp ngoặc đơn hợp lệ
                    if record.count('(') == record.count(')'):

                        record = record.replace("null", "None") 
                        record_tuple = eval(record)  # Chuyển chuỗi thành tuple
                        batch.append(record_tuple)
                    else:
                        print(f"Lỗi định dạng bản ghi: {record}")

                except Exception as e:
                    print(f"Lỗi phân tích bản ghi: {record}. Lỗi: {e}")

                if len(batch) >= batch_size:
                    num_columns = len(record_tuple)
                    placeholders = ', '.join(['?'] * num_columns)  # Tạo chuỗi ? theo số cột
                    query = f"""INSERT INTO {table_name}
                            VALUES ({placeholders})"""
                    cursor.executemany(query, batch)
                    cursor.connection.commit()
                    batch = []

                record = ""  # Reset lại record cho bản ghi tiếp theo

        # Insert phần còn lại nếu có
        if batch:
            num_columns = len(record_tuple)
            placeholders = ', '.join(['?'] * num_columns)  # Tạo chuỗi ? theo số cột
            query = f"""INSERT INTO {table_name} 
                        VALUES ({placeholders})"""
            cursor.executemany(query, batch)
            cursor.connection.commit()

    conn.close()
    
# Hàm main để kiểm tra kết nối
if __name__ == "__main__":
    # Load cấu hình từ file .ini thông qua load_config từ config.py
    insert_from_file('director_mapping.txt', 'director_mapping', batch_size=1000)
    insert_from_file('genre.txt', 'genre', batch_size=1000)
    insert_from_file('movie.txt', 'movie', batch_size=1000)
    insert_from_file('names.txt', 'names', batch_size=1000)
    insert_from_file('ratings.txt', 'ratings', batch_size=1000)
    insert_from_file('role_mapping.txt', 'role_mapping', batch_size=1000)
    