import random
import pandas as pd
from config import SAMPLING_INTERVAL, RANDOM_TYPE_VALUE

def perform_systematic_sampling(all_households_df):
    """
    เลือกครัวเรือนตัวอย่างจาก DataFrame ของครัวเรือนทั้งหมด
    :param all_households_df: DataFrame ที่มีคอลัมน์ 'sort_number_generated' และ 'total_in_ea'
    :return: DataFrame ของครัวเรือนที่ถูกเลือก พร้อมคอลัมน์ใหม่ๆ
    """
    if all_households_df.empty:
        return pd.DataFrame()

    selected_rows_list = []
    print("\nกำลังเริ่มกระบวนการสุ่มเลือกครัวเรือน...")

    # จัดกลุ่มข้อมูลตาม EA
    grouped = all_households_df.groupby(['RegCode', 'AreaCode', 'EA_No'])

    for _, group_df in grouped:
        total_households = group_df['total_in_ea'].iloc[0]

        if total_households <= 0:
            continue

        # สุ่มเลขเริ่มต้น (R) สำหรับกลุ่ม EA นี้
        random_start_value = random.randint(1, SAMPLING_INTERVAL)

        # สร้างลิสต์ของลำดับที่ (sort_number) ที่จะถูกเลือก
        k = 0
        target_sequences = []
        while True:
            seq = random_start_value + (SAMPLING_INTERVAL * k)
            if seq <= total_households:
                target_sequences.append(seq)
                k += 1
            else:
                break
        
        # คัดเลือกแถวที่มี sort_number_generated ตรงกับในลิสต์
        selected_group = group_df[group_df['sort_number_generated'].isin(target_sequences)].copy()
        
        # เพิ่มคอลัมน์ค่าสุ่มเริ่มต้น (R)
        selected_group['RandomStartValue'] = random_start_value
        
        # คัดลอกค่าลำดับที่ที่สร้างขึ้นไปยังคอลัมน์ sort_number ของตารางผลลัพธ์
        selected_group['sort_number'] = selected_group['sort_number_generated']

        selected_rows_list.append(selected_group)
    
    if not selected_rows_list:
        print("ไม่พบครัวเรือนที่ถูกเลือก")
        return pd.DataFrame()

    # รวมผลลัพธ์จากทุกกลุ่มเป็น DataFrame เดียว
    final_selected_df = pd.concat(selected_rows_list, ignore_index=True)
    
    # เพิ่มคอลัมน์ random_type พร้อมกำหนดค่าจาก config
    final_selected_df['random_type'] = RANDOM_TYPE_VALUE
    
    # ลบคอลัมน์ชั่วคราวที่ไม่ต้องการบันทึก
    final_selected_df.drop(columns=['sort_number_generated', 'total_in_ea'], inplace=True)
    
    print(f"กระบวนการสุ่มเสร็จสิ้น พบครัวเรือนตัวอย่างทั้งหมด {len(final_selected_df)} ครัวเรือน")
    return final_selected_df
