import os
import json
import asyncio
import aiohttp
from aiohttp import ClientSession
from datetime import datetime

PRODUCT_ID_FILE = "Project2.txt"
OUTPUT_DIR = "output_files/"
PROCESSED_LOG = "processed_ids.log"
FAILED_LOG = "failed_products.log"
API_URL = "https://api.tiki.vn/product-detail/api/v1/products/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_CONCURRENT_REQUESTS = 50  # Số kết nối đồng thời

# Tạo thư mục output nếu chưa tồn tại
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def normalize_description(description):
    if description:
        description = description.replace("\u003C", "<").replace("\u003E", ">")
        description = description.replace("\u003Cp\u003E", "").replace("\u003C/p\u003E", "\n")
        return description.strip()
    return ""

def log_processed_id(product_id):
    with open(PROCESSED_LOG, "a", encoding="utf-8") as log_file:
        log_file.write(f"{product_id}\n")

def log_failed_product(product_id):
    with open(FAILED_LOG, "a", encoding="utf-8") as log_file:
        log_file.write(f"{product_id}\n")

async def fetch_product_data(session: ClientSession, product_id: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            try:
                async with session.get(f"{API_URL}{product_id}", headers=HEADERS, timeout=10) as response:
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
                        await asyncio.sleep(5)
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

async def process_chunk(chunk, chunk_index, semaphore):
    results = []
    print(f"Processing chunk {chunk_index + 1}...")

    async with ClientSession() as session:
        tasks = [fetch_product_data(session, product_id, semaphore) for product_id in chunk]
        for task in asyncio.as_completed(tasks):
            product_data = await task
            if product_data:
                results.append(product_data)

    # Tạo tên file đầu ra riêng biệt
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{OUTPUT_DIR}products_{chunk_index + 1}_optimized_{timestamp}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(results)} products to {output_file}")

async def main(start_chunk, end_chunk):
    try:
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

        # Chia nhỏ danh sách ID thành các chunk
        chunks = [product_ids[i:i + 100] for i in range(0, len(product_ids), 100)]

        # Chỉ xử lý các chunk trong phạm vi start_chunk đến end_chunk
        chunks = chunks[start_chunk - 1:end_chunk]

        # Semaphore để giới hạn số lượng kết nối đồng thời
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        # Xử lý từng chunk
        for chunk_index, chunk in enumerate(chunks):
            await process_chunk(chunk, chunk_index + start_chunk, semaphore)

        print("Processing completed!")
    except KeyboardInterrupt:
        print("\nProgram interrupted by user. Exiting safely...")

if __name__ == "__main__":
    # Chạy từ chunk 841 đến 2000
    asyncio.run(main(start_chunk=841, end_chunk=2000))