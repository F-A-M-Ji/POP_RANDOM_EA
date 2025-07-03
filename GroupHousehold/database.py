import pandas as pd
from sqlalchemy import create_engine, text, types
from urllib.parse import quote_plus
from config import (
    DB_CONFIG, POSSIBLE_DRIVERS, SOURCE_TABLE, DESTINATION_TABLE,
    SAMPLING_FILTERS, HOUSEHOLD_ORDERING_COLUMN, PARTITION_BY_COLUMNS
)

def get_db_engine():
    """สร้าง SQLAlchemy engine โดยลองเชื่อมต่อด้วยไดรเวอร์หลายตัว"""
    for driver in POSSIBLE_DRIVERS:
        try:
            print(f"กำลังลองเชื่อมต่อด้วยไดรเวอร์: '{driver}'...")
            conn_str = f"mssql+pyodbc://{DB_CONFIG['username']}:{quote_plus(DB_CONFIG['password'])}@{DB_CONFIG['server']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?driver={driver.replace(' ', '+')}"
            engine = create_engine(conn_str, fast_executemany=True, connect_args={'timeout': 10})
            with engine.connect():
                print(f"เชื่อมต่อสำเร็จโดยใช้ไดรเวอร์: '{driver}'!")
                return engine
        except Exception:
            continue
    print("ไม่สามารถเชื่อมต่อฐานข้อมูลได้: ไม่มีไดรเวอร์ที่ใช้งานได้")
    return None

def fetch_data_for_sampling(engine):
    """ดึงข้อมูลครัวเรือนกลุ่มบุคคล พร้อมสร้างลำดับที่และนับจำนวนรวมในแต่ละกลุ่ม"""
    try:
        # สร้าง WHERE clause แบบไดนามิก
        conditions = []
        for key, value in SAMPLING_FILTERS.items():
            if isinstance(value, list):
                in_values = ", ".join([f"'{v}'" for v in value])
                conditions.append(f"[{key}] IN ({in_values})")
            else:
                conditions.append(f"[{key}] = '{value}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # สร้าง SQL query โดยใช้ PARTITION BY ตามที่กำหนดใน config
        # การใส่ [] รอบชื่อคอลัมน์ช่วยให้ปลอดภัยหากชื่อคอลัมน์มีอักขระพิเศษ
        partition_cols = ", ".join([f"[{col}]" for col in PARTITION_BY_COLUMNS])

        sql_query = f"""
            WITH NumberedHouseholds AS (
                SELECT *,
                    -- ส่วนนี้คือหัวใจของการจัดลำดับใหม่ โดยเริ่มนับ 1 ใหม่ทุกครั้งที่ค่าใน RegCode หรือ AreaCode เปลี่ยนไป
                    ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY [{HOUSEHOLD_ORDERING_COLUMN}]) AS sort_number_generated,
                    -- ส่วนนี้นับจำนวนรายการทั้งหมดในแต่ละกลุ่ม (RegCode, AreaCode)
                    COUNT(*) OVER (PARTITION BY {partition_cols}) AS total_in_group
                FROM dbo.[{SOURCE_TABLE}]
                WHERE {where_clause}
            )
            SELECT * FROM NumberedHouseholds;
        """
        print(f"กำลังดึงข้อมูล 'ครัวเรือนกลุ่มบุคคล' (เงื่อนไข: {where_clause})...")
        df = pd.read_sql(sql_query, engine)
        print(f"ดึงข้อมูลที่ผ่านเงื่อนไขทั้งหมด {len(df)} รายการเรียบร้อยแล้ว")
        return df
    except Exception as e:
        print(f"เกิดข้อผิดพลาดขณะดึงข้อมูล: {e}")
        return None

def save_sampling_results(engine, results_df):
    """บันทึก DataFrame ผลลัพธ์ทั้งหมดลงในตารางปลายทาง"""
    if results_df.empty:
        print("ไม่มีข้อมูลสำหรับบันทึก")
        return
    try:
        # กำหนด data type ของคอลัมน์ที่เป็น object (string) ให้เป็น NVARCHAR เพื่อความเข้ากันได้กับ SQL Server
        dtype_mapping = {c: types.NVARCHAR for c, t in results_df.dtypes.items() if isinstance(t, object)}
        with engine.connect() as connection:
            with connection.begin():
                # หากต้องการล้างข้อมูลเก่าก่อนทุกครั้ง ให้ uncomment บรรทัดด้านล่าง
                # print(f"\nกำลังล้างข้อมูลเก่าในตาราง {DESTINATION_TABLE}...")
                # connection.execute(text(f"TRUNCATE TABLE dbo.[{DESTINATION_TABLE}]"))
                print(f"กำลังล้างข้อมูลเก่าในตาราง {DESTINATION_TABLE}...")
                connection.execute(text(f"DELETE FROM {DESTINATION_TABLE} WHERE HouseholdType = 2"))
                print(f"เตรียมบันทึกผลลัพธ์ทั้งหมดจำนวน {len(results_df)} รายการ...")
                results_df.to_sql(DESTINATION_TABLE, con=connection, if_exists='append', index=False, dtype=dtype_mapping, chunksize=1000)
        print("บันทึกข้อมูลทั้งหมดเรียบร้อยแล้ว!")
    except Exception as e:
        print(f"เกิดข้อผิดพลาดขณะบันทึกผลลัพธ์: {e}")
