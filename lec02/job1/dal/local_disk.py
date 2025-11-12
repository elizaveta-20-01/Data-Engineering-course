import os
import json
import shutil
from typing import List, Dict, Any

def save_to_disk(json_content: List[Dict[str, Any]], raw_dir: str) -> None:
   
    # make dir if not exists
    os.makedirs(raw_dir, exist_ok=True)

    # идемпотентность
    for filename in os.listdir(raw_dir):
        file_path = os.path.join(raw_dir, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path) 
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path) 

    # save each record as a separate json file
    for i, record in enumerate(json_content, start=1):
        file_path = os.path.join(raw_dir, f"sales_{i}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(json_content)} records to {raw_dir}")
