from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas as pd


#create connetion
MYSQL_CONNECTION = "mysql_default"

#set path
mysql_output_path = "/home/airflow/gcs/data/raw_data_orders.parquet"
final_output_path = "/home/airflow/gcs/data/orders_data.parquet"


default_args = {
    'owner': 'ecommerce_data'
}

#task1 เพื่อ Query ข้อมูลจาก MySQL
@task()
def get_data_from_mysql(output_path):
    mysqlserver = MySqlHook(MYSQL_CONNECTION)

    # Query จาก database โดยใช้ Hook ที่สร้าง ผลลัพธ์ได้ pandas DataFrame ใช้.get_pandas_df ทำให้สามารถ query ออกมาเป็น datafame และprocess ข้อมูลแบบpandasได้เลย
    list_orders = mysqlserver.get_pandas_df(sql="SELECT * FROM list_orders")
    order_details = mysqlserver.get_pandas_df(sql="SELECT * FROM order_details")
    sales_target = mysqlserver.get_pandas_df(sql="SELECT * FROM sales_target")
    # merged list_orders and order_details
    merged_order = pd.merge(list_orders,order_details,on='Order ID',how='inner')

    # แปลง Order Date เป็น datetime object
    merged_order['Order Date'] = pd.to_datetime(merged_order['Order Date'], format='%d-%m-%Y')
    # สร้างคอลัมน์ใหม่ Month-Year เพื่อนำไป join
    merged_order['Month-Year'] = merged_order['Order Date'].dt.strftime('%b-%y')

    raw_data_orders = pd.merge(merged_order,sales_target,left_on=['Category', 'Month-Year'],right_on=['Category', 'Month of Order Date'], how='outer')
    #เซฟเป็นไฟล์ parquet
    raw_data_orders.to_parquet(output_path, index=False)
    print(f"Output to {output_path}")

#task2 ทำการ transfrom ข้อมูล   
@task()
def transform_data(input_path, output_path):
    raw_data_orders = pd.read_parquet(input_path) #input_path รับข้อมูลมาจาก mysql_output_path ,output_path ส่งออก final_output_path 
    
    #เปลี่ยนชื่อ column
    raw_data_orders.columns = ['order_id','date','customer_name','state','city','amount','profit','quantity','category','sub_category','month_year','month_of_order_date','target']

    #drop ค่า month_year ทิ้ง
    raw_data_orders = raw_data_orders.drop(columns=['month_year'])
    
    #สร้าง column sales เพิ่ม
    raw_data_orders['sales'] = raw_data_orders['amount'] * raw_data_orders['quantity']

    #save เป็นไฟล์ parquet
    raw_data_orders.to_parquet(output_path, index=False)
    print(f"Output to {output_path}")
    

#สร้าง DAG
@dag(default_args=default_args,schedule_interval="@once", start_date=days_ago(1), tags=["project"])
def ecommerce_data():
    t1 = get_data_from_mysql(output_path=mysql_output_path)
    t2 = transform_data(input_path=mysql_output_path, output_path=final_output_path)
    t3 = GCSToBigQueryOperator(
        task_id = "load_orders_data_to_bigquery",
        bucket = "us-central1-pipeline-ecomme-2c1b5da1-bucket",
        source_objects= ["data/orders_data.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table="data.orders",
        write_disposition="WRITE_TRUNCATE"
    )

    t1 >> t2 >> t3
    

ecommerce_data()
    





    
    
