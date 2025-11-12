from lec02.job1.dal import local_disk, sales_api
from typing import List, Dict, Any

def save_sales_to_local_disk(date: str, raw_dir: str) -> None:

    print(f"Fetching sales data for {date}...")

    #get
    sales_data: List[Dict[str, Any]] = sales_api.get_sales(date)
    print(f"Fetched {len(sales_data)} records.")

    #save
    local_disk.save_to_disk(sales_data, raw_dir)
    print(f"Saved sales data to {raw_dir}")
