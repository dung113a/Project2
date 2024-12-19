import os
import json
import asyncio
import aiohttp
from aiohttp import ClientSession
from datetime import datetime

# Cấu hình
PRODUCT_ID_FILE = "Project2_remaining.txt"  # File chứa danh sách ID sản phẩm
OUTPUT_DIR = "output_files2/"
PROCESSED_LOG = "processed_ids.log"
FAILED_LOG = "failed_products.log"
API_URL = "https://api.tiki.vn/product-detail/api/v1/products/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_CONCURRENT_REQUESTS = 50  # Số kết nối đồng thời tối đa
CHUNK_SIZE = 1000  # Số ID mỗi chunk (đã thay đổi từ 100 thành 1000)
MAX_RETRIES = 5  # Số lần retry tối đa
RETRY_DELAY = 10  # Thời gian chờ giữa các lần retry (giây)

# Tạo thư mục output nếu chưa tồn tại
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Hàm chuẩn hóa description
def normalize_description(description):
    if description:
        description = description.replace("\u003C", "<").replace("\u003E", ">")
        description = description.replace("\u003Cp\u003E", "").replace("\u003C/p\u003E", "\n")
        return description.strip()
    return ""

# Ghi log ID đã xử lý
def log_processed_id(product_id):
    with open(PROCESSED_LOG, "a", encoding="utf-8") as log_file:
        log_file.write(f"{product_id}\n")

# Ghi log sản phẩm bị lỗi
def log_failed_product(product_id):
    with open(FAILED_LOG, "a", encoding="utf-8") as log_file:
        log_file.write(f"{product_id}\n")

# Hàm fetch dữ liệu sản phẩm
async def fetch_product_data(session: ClientSession, product_id: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                async with session.get(f"{API_URL}{product_id}", headers=HEADERS, timeout=15) as response:
                    if response.status == 200:
                        data = await response.json()
                        log_processed_id(product_id)
                        return {
                            "id": data.get("id"),
                            "name": data.get("name"),
                            "url_key": data.get("url_key"),
                            "price": data.get("price"),
                            "description": normalize_description(data.get("description")),
                            "images_url": [img.get("large_url") for img in data.get("images", [])]
                        }
                    elif response.status == 429:
                        print(f"Rate limit exceeded for product {product_id}. Retrying...")
                        await asyncio.sleep(RETRY_DELAY)
                        retry_count += 1
                    else:
                        print(f"Failed to fetch product {product_id}: {response.status}")
                        log_failed_product(product_id)
                        return None
            except asyncio.TimeoutError:
                print(f"Timeout for product {product_id}. Retrying...")
                retry_count += 1
            except Exception as e:
                print(f"Error fetching product {product_id}: {e}")
                log_failed_product(product_id)
                return None
        print(f"Max retries reached for product {product_id}. Skipping...")
        log_failed_product(product_id)
        return None

# Hàm xử lý từng chunk
async def process_chunk(chunk, chunk_index, semaphore):
    results = []
    print(f"Processing chunk {chunk_index + 1}...")

    async with ClientSession() as session:
        tasks = [fetch_product_data(session, product_id, semaphore) for product_id in chunk]
        for task in asyncio.as_completed(tasks):
            product_data = await task
            if product_data:
                results.append(product_data)

    # In log kết quả chunk
    print(f"Chunk {chunk_index + 1}: {len(results)} products fetched out of {len(chunk)}")

    if results:
        # Tạo tên file đầu ra riêng biệt
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{OUTPUT_DIR}products_{chunk_index + 1}_optimized_{timestamp}.json"
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"Saved {len(results)} products to {output_file}")
        except Exception as e:
            print(f"Error saving chunk {chunk_index + 1} to file: {e}")
    else:
        print(f"Chunk {chunk_index + 1} has no valid products. Skipping file creation.")

# Hàm chính
async def main():
    try:
        # Đọc toàn bộ danh sách ID từ file
        with open(PRODUCT_ID_FILE, "r", encoding="utf-8") as f:
            product_ids = [line.strip() for line in f.readlines()]

        # Đọc danh sách ID đã xử lý
        if os.path.exists(PROCESSED_LOG):
            with open(PROCESSED_LOG, "r", encoding="utf-8") as log_file:
                processed_ids = set(log_file.read().splitlines())
        else:
            processed_ids = set()

        # Lọc các ID chưa xử lý
        product_ids = [pid for pid in product_ids if pid not in processed_ids]

        # Chia nhỏ danh sách ID thành các chunk (mỗi chunk 1000 ID)
        chunks = [product_ids[i:i + CHUNK_SIZE] for i in range(0, len(product_ids), CHUNK_SIZE)]

        # Semaphore để giới hạn số lượng kết nối đồng thời
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        # Xử lý từng chunk
        for chunk_index, chunk in enumerate(chunks):
            await process_chunk(chunk, chunk_index, semaphore)

        print("Processing completed!")
    except KeyboardInterrupt:
        print("\nProgram interrupted by user. Exiting safely...")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Chạy chương trình
    asyncio.run(main())
