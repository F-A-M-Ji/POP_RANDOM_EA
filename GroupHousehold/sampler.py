import random
import pandas as pd
from config import SAMPLING_INTERVAL, RANDOM_TYPE_VALUE, PARTITION_BY_COLUMNS

def perform_systematic_sampling(households_df):
    """
    ทำการสุ่มตัวอย่างแบบมีระบบ (Systematic Sampling) ในแต่ละกลุ่มย่อย
    ที่กำหนดโดย PARTITION_BY_COLUMNS
    """
    if households_df.empty:
        return pd.DataFrame()

    selected_rows_list = []
    print("\nกำลังเริ่มสุ่มเลือก 'ครัวเรือนกลุ่มบุคคล'...")
    
    # ทำการจัดกลุ่มข้อมูล (grouping) ตามคอลัมน์ที่กำหนดไว้ใน config
    # ณ จุดนี้ DataFrame จะถูกแบ่งออกเป็นกลุ่มย่อยๆ ตาม (RegCode, AreaCode) ที่ไม่ซ้ำกัน
    grouped = households_df.groupby(PARTITION_BY_COLUMNS)

    # วนลูปเพื่อทำการสุ่มในแต่ละกลุ่มย่อย
    for group_key, group_df in grouped:
        total_in_group = group_df['total_in_group'].iloc[0]
        if total_in_group <= 0:
            continue

        # สุ่มหาลำดับเริ่มต้น (Random Start)
        random_start_value = random.randint(1, SAMPLING_INTERVAL)
        
        # คำนวณหาลำดับที่ทั้งหมดที่จะถูกเลือกในกลุ่มนี้
        target_sequences = [
            seq for k in range(total_in_group) 
            if (seq := random_start_value + (SAMPLING_INTERVAL * k)) <= total_in_group
        ]
        
        # เลือกแถวที่มีลำดับตรงกับ target_sequences
        selected_group = group_df[group_df['sort_number_generated'].isin(target_sequences)].copy()
        
        if not selected_group.empty:
            # เพิ่มคอลัมน์สำหรับบันทึกค่าที่ใช้ในการสุ่ม
            selected_group['RandomStartValue'] = random_start_value
            selected_group['sort_number'] = selected_group['sort_number_generated']
            selected_group['random_type'] = RANDOM_TYPE_VALUE
            selected_rows_list.append(selected_group)
    
    if not selected_rows_list:
        print("ไม่พบครัวเรือนที่ถูกเลือก")
        return pd.DataFrame()

    # รวมผลลัพธ์จากทุกกลุ่มเข้าด้วยกัน
    final_selected_df = pd.concat(selected_rows_list, ignore_index=True)
    
    print(f"สุ่มเลือกสำเร็จ พบครัวเรือนตัวอย่าง {len(final_selected_df)} ครัวเรือน")
    return final_selected_df
