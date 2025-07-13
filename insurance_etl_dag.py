from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
import pandas as pd
import logging # สำหรับ logging ข้อความ

# ตัวแปรของ output_path ที่จะเซฟ
extract_path = "/home/airflow/gcs/data/motor_insurance.csv"
final_output_path = "/home/airflow/gcs/data/motor_insurance_output.parquet"

default_args = {
    'owner': 'wittawat',
}

@task()
def clean_data(insurance_path, output_path):
    try:
        # อ่านไฟล์ csv ที่ extract มาตาม insurance_path
        # sep=';' ระบุตัวคั่น
        insurance_df = pd.read_csv(insurance_path, sep=';')
        logging.info(f"Successfully loaded CSV. Original DataFrame shape: {insurance_df.shape}")
    except FileNotFoundError:
        logging.error(f"Error: CSV file not found at {insurance_path}. Make sure it was downloaded correctly.")
        raise
    except Exception as e:
        logging.error(f"Error reading CSV into Pandas: {e}")
        raise
    
    # สร้าง copy ของ DataFrame เพื่อไม่ให้กระทบกับ original
    insurance_clean_df = insurance_df.copy()
    
    # --- จัดการ missing value ---
    # จัดการ Type_fuel ที่มี missing value เป็น Unknown
    insurance_clean_df['Type_fuel'].fillna('Unknown', inplace=True)
    logging.info(f"Missing values in Type_fuel after cleaning: {insurance_clean_df['Type_fuel'].isnull().sum()}")
    
    # จัดการ Length ที่มี missing value เป็น ค่าเฉลี่ย
    mean_length = insurance_clean_df['Length'].mean()
    insurance_clean_df['Length'].fillna(mean_length, inplace=True)
    logging.info(f"Missing values in Length after cleaning: {insurance_clean_df['Length'].isnull().sum()}")
    
    # --- จัดการ Domain Format Error ของ Column Distribution_channel รวมถึง แปลง Data Type ไปเป็น int ---
    # แทนที่ '00/01/1900' ด้วย 0 แล้ว convert data type ไปเป็น int
    insurance_clean_df['Distribution_channel'] = insurance_clean_df['Distribution_channel'].replace('00/01/1900',0)
    insurance_clean_df['Distribution_channel'] = insurance_clean_df['Distribution_channel'].astype(int)
    logging.info(f"Unique values in Distribution_channel after cleaning: {insurance_clean_df['Distribution_channel'].unique()}")
    
    # --- ทำการ convert Data Type คอลัมน์ที่เป็น วันที่ ไปเป็น date time ---
    date_columns = ['Date_start_contract', 'Date_last_renewal', 'Date_next_renewal', 'Date_birth', 'Date_driving_licence', 'Date_lapse']  # แก้ไขชื่อตามที่ต้องการ

    for col in date_columns:
        # errors='coerce' จะแปลงค่าที่ไม่สามารถเป็นวันที่ได้ให้เป็น NaT (Not a Time)
        # dayfirst=True สำคัญมากเพื่อให้ตีความถูกต้อง (วัน/เดือน/ปี)
        insurance_clean_df[col] = pd.to_datetime(insurance_clean_df[col], errors='coerce', dayfirst=True)
        
        # กรองค่าที่ต่ำเกินไป (BigQuery รองรับจาก 0001-01-01 เป็นต้นไป)
        ts_limit = pd.Timestamp("0001-01-01")
        insurance_clean_df.loc[insurance_clean_df[col] < ts_limit, col] = pd.NaT
        
        # เพิ่ม timezone เป็น UTC เพื่อให้ BigQuery detect เป็น TIMESTAMP
        insurance_clean_df[col] = insurance_clean_df[col].dt.tz_localize("UTC")
        logging.info(f"Converted '{col}' to datetime. Data Type: {insurance_clean_df[col].dtype}. Nulls after conversion: {insurance_clean_df[col].isnull().sum()}")
    
    # --- จัดการ Logical Contradictions เงื่อนไขต่างๆ ---
    #เงื่อนไข 5 (Date_last_renewal > Date_lapse)
    #Lapse = 0 แล้ว Date_lapse มีค่า ให้กำหนด Date_lapse = NaT
    #Lapse = 1, 2 (มีการยกเลิกสัญญาแล้ว) แต่มีการต่ออายุสัญญาหลังจากวันยกเลิก จะตั้งค่า Date_last_renewal เป็น NaT
    insurance_clean_df.loc[(insurance_clean_df['Lapse'] == 0) & (insurance_clean_df['Date_lapse'].notnull()),'Date_lapse'] = pd.NaT
    insurance_clean_df.loc[(insurance_clean_df['Lapse'].isin([1,2])) & (insurance_clean_df['Date_lapse'].notnull()) & (insurance_clean_df['Date_last_renewal'] > insurance_clean_df['Date_lapse']),'Date_last_renewal'] = pd.NaT
    logging.info(f"Corrected contradiction 5: {len(insurance_clean_df[insurance_clean_df['Date_last_renewal'] > insurance_clean_df['Date_lapse']])} rows.")
    
    #เงื่อนไข 1 (Date_start_contract > Date_last_renewal)
    # Flag แถวเหล่านี้ไว้เพื่อระบุว่าเป็นความผิดปกติทางตรรกะ (logical inconsistency)
    insurance_clean_df['flag_invalid_start_vs_last'] = (
        insurance_clean_df['Date_start_contract'] > insurance_clean_df['Date_last_renewal']
    ).astype(int)
    logging.info(f"Flagged inconsistent start contract > last renewal dates: {insurance_clean_df['flag_invalid_start_vs_last'].sum()} rows.")
    
    # save ไฟล์ Parquet
    logging.info(f"Saving cleaned data to local Parquet file: {output_path}")
    try:
        insurance_clean_df.to_parquet(output_path,engine="pyarrow", index=False, coerce_timestamps="ms",allow_truncated_timestamps=True)
        logging.info("Parquet file saved successfully to local.")
    except Exception as e:
        logging.error(f"Error saving Parquet file locally: {e}")
        raise
    


@dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1), catchup=False, tags=["insurance_etl"])
def insurance_data_etl_pipeline():
    """
    # เป็น etl ข้อมูลประกันภัย ซึ่งมีการ extract มาจาก source แล้วทำการ Clen data 
    จากนั้นจะทำการ load ข้อมูลเข้า BigQuery ผ่าน GCSToBigQueryOperator ของ Airflow
    """
    
    t1 = GCSToLocalFilesystemOperator(
        task_id="extract_csv_from_gcs",
        bucket="insurance-source",
        object_name="Motor_vehicle_insurance_data.csv",
        filename=extract_path
    )
    
    t2 = clean_data(insurance_path=extract_path, output_path=final_output_path)

    # TODO: สร้าง t4 ที่เป็น GCSToBigQueryOperator เพื่อใช้งานกับ BigQuery แบบ Airflow และใส่ dependencies
    t3 = GCSToBigQueryOperator(
        task_id = "gcs_to_bigquery",
        bucket = "us-central1-insurance-etl-5fa97e7b-bucket",
        source_objects = ["data/motor_insurance_output.parquet"],
        source_format = "PARQUET",
        destination_project_dataset_table = "motor_insurance.insurance",
        write_disposition = "WRITE_TRUNCATE"
    )

    t1 >> t2 >> t3

insurance_data_etl_pipeline()