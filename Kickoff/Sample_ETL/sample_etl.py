import pandas as pd
import logging
from sqlalchemy import create_engine
from typing import Dict

# BEST PRACTICE: Thiết lập logging ngay từ đầu
# - Format rõ ràng với timestamp
# - Capture cả INFO và ERROR levels
# - Giúp debug và monitor quá trình ETL
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_db_engine(db_url: str):
    """
    Tạo database connection engine

    OPTIMIZATION:
    - Connection pooling giúp tái sử dụng connections
    - Giảm overhead tạo connection mới
    - Xử lý concurrent operations tốt hơn
    """
    try:
        # BEST PRACTICE: Cấu hình connection pool
        # pool_size=5: Số connection tối thiểu
        # max_overflow=10: Số connection có thể tạo thêm khi cần
        engine = create_engine(
            db_url,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600  # Recycle connections sau 1 giờ
        )
        logger.info("Kết nối database thành công")
        return engine
    except Exception as e:
        logger.error(f"Lỗi kết nối database: {e}")
        raise


def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tối ưu kiểu dữ liệu của DataFrame

    OPTIMIZATION & BEST PRACTICES:
    1. Memory Usage:
       - Giảm size của numeric columns
       - Chuyển object thành category khi thích hợp
    2. Processing Speed:
       - Dùng kiểu dữ liệu nhỏ hơn giúp tính toán nhanh hơn
    """
    memory_before = df.memory_usage().sum() / 1024 ** 2

    for col in df.columns:
        # OPTIMIZATION: Convert object to category
        if df[col].dtype == 'object':
            # BEST PRACTICE: Chỉ convert khi có lợi về memory
            unique_ratio = df[col].nunique() / len(df)
            if unique_ratio < 0.5:  # Ít hơn 50% giá trị unique
                df[col] = df[col].astype('category')

        # OPTIMIZATION: Downcast numeric types
        elif df[col].dtype == 'int64':
            # BEST PRACTICE: Sử dụng to_numeric với downcast
            df[col] = pd.to_numeric(df[col], downcast='integer')

        elif df[col].dtype == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float')

    # BEST PRACTICE: Log memory optimization results
    memory_after = df.memory_usage().sum() / 1024 ** 2
    logger.info(f"Memory reduced from {memory_before:.2f}MB to {memory_after:.2f}MB")

    return df


def extract(file_path: str) -> pd.DataFrame:
    """
    EXTRACT: Đọc dữ liệu từ file CSV

    OPTIMIZATION & BEST PRACTICES:
    1. Memory Management:
       - Đọc file theo chunks để xử lý file lớn
       - Tối ưu datatypes
    2. Performance:
       - Sử dụng chunk size phù hợp
       - Avoid loading entire file vào memory
    """
    try:
        logger.info(f"Bắt đầu đọc file: {file_path}")

        # OPTIMIZATION: Chunked reading
        # - chunk_size=10000: Tối ưu cho most cases
        # - Tránh memory overflow với file lớn
        chunks = []
        for chunk in pd.read_csv(
                file_path,
                chunksize=10000,
                # BEST PRACTICE: Định nghĩa dtype cho columns quan trọng
                dtype={
                    'id': 'int32',
                    'status': 'category'
                }
        ):
            chunks.append(chunk)

        # BEST PRACTICE: Sử dụng ignore_index khi concat
        df = pd.concat(chunks, ignore_index=True)

        # OPTIMIZATION: Memory usage
        df = optimize_dtypes(df)

        # BEST PRACTICE: Log kết quả chi tiết
        logger.info(f"Đọc file thành công. Shape: {df.shape}")
        logger.info(f"Memory usage: {df.memory_usage().sum() / 1024 ** 2:.2f} MB")

        return df

    except Exception as e:
        logger.error(f"Lỗi đọc file: {e}")
        raise


def transform(df: pd.DataFrame, fill_values: Dict = None) -> pd.DataFrame:
    """
    TRANSFORM: Xử lý và làm sạch dữ liệu

    OPTIMIZATION & BEST PRACTICES:
    1. Performance:
       - Sử dụng vectorized operations
       - Chain operations để giảm số lần quét DataFrame
    2. Memory:
       - Inplace operations khi có thể
    3. Data Quality:
       - Xử lý missing values
       - Remove duplicates
    """
    try:
        logger.info("Bắt đầu transform dữ liệu")

        # BEST PRACTICE: Default values cho missing data
        if fill_values is None:
            fill_values = {
                'numeric_col': 0,
                'text_col': 'unknown'
            }

        # OPTIMIZATION: Chain operations
        # - Giảm số lần quét DataFrame
        # - Tối ưu memory usage
        df = (df
              .drop_duplicates()  # Xóa duplicates trước
              .fillna(fill_values)  # Sau đó fill missing values
              )

        # BEST PRACTICE: Chuẩn hóa column names
        # - Lowercase để consistency
        # - Replace spaces với underscores
        df.columns = df.columns.str.lower().str.replace(' ', '_')

        # BEST PRACTICE: Log transformation results
        logger.info(f"Số rows sau khi remove duplicates: {len(df)}")
        logger.info("Transform dữ liệu thành công")

        return df

    except Exception as e:
        logger.error(f"Lỗi transform dữ liệu: {e}")
        raise


def load(df: pd.DataFrame,
         db_url: str,
         table_name: str,
         if_exists: str = 'replace') -> None:
    """
    LOAD: Đẩy dữ liệu vào database

    OPTIMIZATION & BEST PRACTICES:
    1. Performance:
       - Batch inserts với chunk size tối ưu
       - Connection pooling
    2. Memory:
       - Stream data
    3. Reliability:
       - Error handling cho từng chunk
    """
    try:
        logger.info(f"Bắt đầu load dữ liệu vào bảng {table_name}")

        engine = create_db_engine(db_url)

        # OPTIMIZATION: Calculate optimal chunk size
        # - Không quá lớn để tránh memory issues
        # - Không quá nhỏ để tối ưu performance
        optimal_chunk_size = min(1000, len(df))

        # BEST PRACTICE: Load từng chunk với error handling
        for i in range(0, len(df), optimal_chunk_size):
            chunk = df.iloc[i:i + optimal_chunk_size]
            try:
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    # OPTIMIZATION: Sử dụng append cho chunks sau chunk đầu
                    if_exists='append' if i > 0 else if_exists,
                    index=False,
                    # OPTIMIZATION: Sử dụng multi-row inserts
                    method='multi'
                )
                # BEST PRACTICE: Log progress
                logger.info(f"Đã load {i + len(chunk)}/{len(df)} dòng")
            except Exception as e:
                logger.error(f"Lỗi khi load chunk {i // optimal_chunk_size + 1}: {e}")
                raise

        logger.info("Load dữ liệu thành công")

    except Exception as e:
        logger.error(f"Lỗi load dữ liệu: {e}")
        raise


def run_etl_pipeline(file_path: str, db_url: str, table_name: str):
    """
    Chạy toàn bộ ETL pipeline

    BEST PRACTICES:
    1. Error Handling:
       - Try-except cho toàn bộ pipeline
       - Logging chi tiết
    2. Monitoring:
       - Track thời gian thực thi
       - Log kết quả từng phase
    """
    try:
        # BEST PRACTICE: Track execution time
        import time
        start_time = time.time()

        # BEST PRACTICE: Log bắt đầu pipeline
        logger.info(f"Starting ETL pipeline: {file_path} -> {table_name}")

        # Extract
        df = extract(file_path)
        logger.info("Extract phase completed")

        # Transform
        df = transform(df)
        logger.info("Transform phase completed")

        # Load
        load(df, db_url, table_name)
        logger.info("Load phase completed")

        # BEST PRACTICE: Log summary
        execution_time = time.time() - start_time
        logger.info(f"ETL pipeline completed in {execution_time:.2f} seconds")

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise


if __name__ == "__main__":
    # BEST PRACTICE: Cấu hình riêng cho từng environment
    config = {
        'file_path': 'data.csv',
        'db_url': 'postgresql://user:pass@localhost:5432/mydb',
        'table_name': 'processed_data'
    }

    try:
        run_etl_pipeline(**config)
    except Exception as e:
        logger.error(f"Error: {e}")
        raise