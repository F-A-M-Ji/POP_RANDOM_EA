import pandas as pd
from sqlalchemy import create_engine, text, types
from urllib.parse import quote_plus
from config import (
    DB_CONFIG, POSSIBLE_DRIVERS, SOURCE_TABLE,
    DESTINATION_TABLE, HOUSEHOLD_ORDERING_COLUMN,
    SAMPLING_FILTERS
)

def get_db_engine():
    """
    สร้าง SQLAlchemy engine โดยลองเชื่อมต่อด้วยไดรเวอร์หลายตัว
    และระบุ charset ใน connection string เพื่อรองรับภาษาไทย
    """
    for driver in POSSIBLE_DRIVERS:
        try:
            print(f"กำลังลองเชื่อมต่อด้วยไดรเวอร์: '{driver}'...")
            # การระบุ &charset=utf8 ใน connection string เป็นวิธีที่ถูกต้องสำหรับไดรเวอร์
            conn_str = (
                f"mssql+pyodbc://{DB_CONFIG['username']}:{quote_plus(DB_CONFIG['password'])}"
                f"@{DB_CONFIG['server']}:{DB_CONFIG['port']}"
                f"/{DB_CONFIG['database']}?driver={driver.replace(' ', '+')}&charset=utf8"
            )
            
            # แก้ไข: นำพารามิเตอร์ 'encoding' ที่ไม่รองรับออกไป
            engine = create_engine(
                conn_str, 
                fast_executemany=True, 
                connect_args={'timeout': 10}
            )
            
            # ทดสอบการเชื่อมต่อ
            with engine.connect():
                print(f"เชื่อมต่อสำเร็จโดยใช้ไดรเวอร์: '{driver}'!")
                return engine
        except Exception as e:
            print(f"ไม่สามารถเชื่อมต่อด้วยไดรเวอร์ '{driver}': {e}")
            continue
            
    print("ไม่สามารถเชื่อมต่อฐานข้อมูลได้: ไม่มีไดรเวอร์ที่ใช้งานได้")
    return None

def fetch_data_for_sampling(engine):
    """
    ดึงข้อมูลครัวเรือนทั้งหมด (พร้อมกรองข้อมูล) และสร้างลำดับที่, นับจำนวนรวมในแต่ละ EA
    """
    try:
        where_conditions = [f"[{key}] = '{value}'" for key, value in SAMPLING_FILTERS.items()]
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

        sql_query = f"""
            WITH NumberedHouseholds AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY RegCode, AreaCode, EA_No
                        ORDER BY [{HOUSEHOLD_ORDERING_COLUMN}]
                    ) AS sort_number_generated,
                    COUNT(*) OVER (
                        PARTITION BY RegCode, AreaCode, EA_No
                    ) AS total_in_ea
                FROM
                    dbo.[{SOURCE_TABLE}]
                WHERE
                    {where_clause}
            )
            SELECT * FROM NumberedHouseholds;
        """
        print("กำลังดึงข้อมูลและจัดลำดับครัวเรือน (พร้อมเงื่อนไขการกรอง)...")
        print(f"เงื่อนไขการกรองที่ใช้: {where_clause}")
        
        df = pd.read_sql(sql_query, engine)
        print(f"ดึงข้อมูลครัวเรือนที่ผ่านเงื่อนไขทั้งหมด {len(df)} รายการเรียบร้อยแล้ว")
        return df
    except Exception as e:
        print(f"เกิดข้อผิดพลาดขณะดึงข้อมูล: {e}")
        return None

def save_sampling_results(engine, results_df):
    """บันทึกผลลัพธ์การสุ่มลงในตารางปลายทาง"""
    if results_df.empty:
        print("ไม่มีข้อมูลสำหรับบันทึก")
        return

    try:
        # การแมปชนิดข้อมูลเป็น NVARCHAR เป็นสิ่งสำคัญเพื่อให้แน่ใจว่าตารางปลายทางรองรับ Unicode
        dtype_mapping = {
            col_name: types.NVARCHAR(length=255) 
            for col_name, col_type in results_df.dtypes.items()
            if pd.api.types.is_string_dtype(col_type) or isinstance(col_type, object)
        }
        
        with engine.connect() as connection:
            with connection.begin():
                # print(f"กำลังล้างข้อมูลเก่าในตาราง {DESTINATION_TABLE}...")
                # connection.execute(text(f"TRUNCATE TABLE {DESTINATION_TABLE}"))
                print(f"เตรียมบันทึกผลลัพธ์จำนวน {len(results_df)} รายการ...")
                results_df.to_sql(
                    DESTINATION_TABLE,
                    con=connection,
                    if_exists='append',
                    index=False,
                    dtype=dtype_mapping,
                    chunksize=1000
                )
        print("บันทึกข้อมูลเรียบร้อยแล้ว!")
    except Exception as e:
        print(f"เกิดข้อผิดพลาดขณะบันทึกผลลัพธ์: {e}")

