import pandas as pd
import database
import sampler
from config import DESTINATION_TABLE

def run_process():
    """ฟังก์ชันหลักสำหรับควบคุมกระบวนการทั้งหมด"""
    engine = None
    try:
        print("--- เริ่มต้นกระบวนการสุ่มตัวอย่าง: 'ครัวเรือนกลุ่มบุคคล' ---")
        engine = database.get_db_engine()
        if not engine:
            return

        # 1. ดึงข้อมูลพร้อมจัดลำดับและนับจำนวนในแต่ละกลุ่ม
        df_data = database.fetch_data_for_sampling(engine)
        
        if df_data is not None and not df_data.empty:
            # 2. ทำการสุ่มตัวอย่างแบบมีระบบในแต่ละกลุ่ม
            selected_df = sampler.perform_systematic_sampling(df_data)

            # 3. จัดการคอลัมน์ก่อนบันทึก
            if not selected_df.empty:
                # ดึง Schema ของตารางปลายทางเพื่อคัดกรองคอลัมน์
                with engine.connect() as connection:
                    dest_table_info = pd.read_sql(f"SELECT TOP 0 * FROM dbo.[{DESTINATION_TABLE}]", connection)
                    dest_columns = dest_table_info.columns.tolist()

                # เลือกเฉพาะคอลัมน์ที่มีอยู่ในตารางปลายทาง
                final_columns = [col for col in selected_df.columns if col in dest_columns]
                final_df_to_save = selected_df[final_columns]
                
                # 4. บันทึกผลลัพธ์
                database.save_sampling_results(engine, final_df_to_save)
            else:
                print("ไม่มีข้อมูลถูกสุ่มเลือกในรอบนี้")
        else:
            print("ไม่พบข้อมูลเริ่มต้นที่จะนำมาประมวลผล")

    except Exception as e:
        print(f"เกิดข้อผิดพลาดร้ายแรงในกระบวนการหลัก: {e}")
    finally:
        if engine:
            engine.dispose()
            print("\nปิดการเชื่อมต่อฐานข้อมูลแล้ว")
        print("--- กระบวนการทั้งหมดเสร็จสมบูรณ์ ---")

if __name__ == "__main__":
    run_process()
