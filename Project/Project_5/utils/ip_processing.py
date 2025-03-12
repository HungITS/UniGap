from concurrent.futures import ThreadPoolExecutor
from config.iplocation_config import ip2loc  # Import IP2Location configuration from ip2location_config.py
from config.db_config import main_collection, location_collection  # Import collections from db_config.py
from utils.save_failed_ips import save_failed_ips

batch_size = 10000  # Number of IPs per batch
num_threads = 10  # Number of threads for parallel processing

def process_ip(ip):
    """Processes a single IP and returns location data or None if an error occurs."""
    try:
        record = ip2loc.get_all(ip)
        return {
            "ip": ip,
            "country": record.country_long,
            "region": record.region,
            "city": record.city,
        }
    except Exception as e:
        print(f"Error processing IP {ip}: {e}")
        return None

def process_batch(batch, batch_num):
    """Processes a batch of IPs and returns successful results and failed IPs."""
    results = []
    failed_ips = []

    print(f"Processing batch {batch_num}... (Size: {len(batch)})")

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_ip = {executor.submit(process_ip, ip): ip for ip in batch}

        for future in future_to_ip:
            result = future.result()
            if result:
                results.append(result)
            else:
                failed_ips.append(future_to_ip[future])

    print(f"Batch {batch_num} completed! Success: {len(results)}, Failed: {len(failed_ips)}")
    return results, failed_ips

def process_ips():
    """Processes all IPs from MongoDB in batches."""
    cursor = main_collection.find({}, {"ip": 1})  # Fetch all IPs

    batch = []
    batch_num = 0

    for doc in cursor:
        batch.append(doc['ip'])

        if len(batch) >= batch_size:
            batch_num += 1
            results, failed_ips = process_batch(batch, batch_num)

            # Store results in MongoDB
            if results:
                location_collection.insert_many(results)

            # Save failed IPs
            if failed_ips:
                save_failed_ips(failed_ips)

            # Clear batch and continue
            batch.clear()

    # Process any remaining data
    if batch:
        batch_num += 1
        results, failed_ips = process_batch(batch, batch_num)
        if results:
            location_collection.insert_many(results)
        if failed_ips:
            save_failed_ips(failed_ips)

    print("All IPs have been processed successfully!")

