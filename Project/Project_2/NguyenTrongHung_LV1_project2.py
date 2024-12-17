import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

API_GET_PRODUCT = 'https://api.tiki.vn/product-detail/api/v1/products/{}'

def normalize_description(description):
    if not description:
        return ""
    soup = BeautifulSoup(description, "html.parser")
    text = soup.get_text()
    text = ' '.join(text.split())
    return text

def extract_image_urls(images_data):
    """Trích xuất tất cả các URL hình ảnh từ danh sách images nếu là URL hợp lệ."""
    if not isinstance(images_data, list):
        return []
    
    images_url = []
    for image in images_data:
        image_urls = {
            "base_url": image.get("base_url", "").replace("\\", ""),
            "large_url": image.get("large_url", "").replace("\\", ""),
            "medium_url": image.get("medium_url", "").replace("\\", ""),
            "small_url": image.get("small_url", "").replace("\\", ""),
            "thumbnail_url": image.get("thumbnail_url", "").replace("\\", ""),
        }
        # Loại bỏ các URL rỗng
        image_urls = {k: v for k, v in image_urls.items() if v}
        images_url.append(image_urls)
    return images_url

def get_data(product_id):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0'
        }
        
        response = requests.get(API_GET_PRODUCT.format(product_id), headers=headers, timeout=10)
        response.raise_for_status()

        if response.status_code == 200:
            product_data = response.json()

            dict_data = {
                "id": product_data.get("id"),
                "name": product_data.get("name"),
                "url_key": product_data.get("url_key"),
                "price": product_data.get("price", {}),
                "description": normalize_description(product_data.get("description")),
                "images": extract_image_urls(product_data.get('images', []))
            }
            return dict_data
    
    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"Get data failed for product ID {product_id}: {e}")
        return None

if __name__ == '__main__':
    output_dir = '/home/hungits/Documents/UniGap/Project/Project_2/json_file'

    list_product_id = pd.read_csv('products-0-200000(in).csv')
    list_product_id = list_product_id.iloc[:, 0].tolist()

    all_products = []
    
    # Chạy đa luồng để xử lý nhiều sản phẩm cùng lúc
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_data, product_id): product_id for product_id in list_product_id}

        for idx, future in enumerate(as_completed(futures), 1):
            product_data = future.result()
            if product_data:
                all_products.append(product_data)

            # Lưu dữ liệu thành file JSON mỗi 1000 sản phẩm
            if idx % 1000 == 0 or idx == len(list_product_id):
                batch_number = (idx - 1) // 1000 + 1
                batch_file = f"{output_dir}/products_batch_{batch_number}.json"
                df = pd.DataFrame(all_products)
                df.to_json(batch_file, orient="records", force_ascii=False, indent=4)
                print(f"Đã lưu {len(all_products)} sản phẩm vào file {batch_file}")
                all_products = []  # Reset danh sách để bắt đầu batch mới