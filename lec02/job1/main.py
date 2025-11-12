from lec02.job1.bll.sales_api import save_sales_to_local_disk

purchase_date = "2022-08-09"
raw_dir = r"C:\Users\User\Projects\raw\sales\2022-08-09"
save_sales_to_local_disk(date=purchase_date, raw_dir=raw_dir)

