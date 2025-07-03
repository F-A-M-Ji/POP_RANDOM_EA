import database
import sampler

def run_process():
    """ฟังก์ชันหลักสำหรับควบคุมกระบวนการทั้งหมด"""
    engine = None
    try:
        print("--- เริ่มต้นกระบวนการสุ่มตัวอย่างครัวเรือน ---")
        engine = database.get_db_engine()
        
        if engine:
            # 1. ดึงข้อมูลทั้งหมดพร้อมจัดลำดับ
            df_all_data = database.fetch_data_for_sampling(engine)
            
            if df_all_data is not None and not df_all_data.empty:
                # 2. ทำการสุ่มเลือกครัวเรือน
                selected_households_df = sampler.perform_systematic_sampling(df_all_data)
                
                # 3. บันทึกผลลัพธ์
                database.save_sampling_results(engine, selected_households_df)
            else:
                print("ไม่สามารถดึงข้อมูลครัวเรือนเพื่อมาประมวลผลได้")

    except Exception as e:
        print(f"เกิดข้อผิดพลาดร้ายแรงในกระบวนการหลัก: {e}")
    finally:
        if engine:
            engine.dispose()
            print("\nปิดการเชื่อมต่อฐานข้อมูลแล้ว")
        print("--- กระบวนการทั้งหมดเสร็จสมบูรณ์ ---")

if __name__ == "__main__":
    run_process()
