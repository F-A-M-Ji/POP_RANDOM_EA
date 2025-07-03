# --- ตั้งค่าการเชื่อมต่อฐานข้อมูล ---
DB_CONFIG = {
    'server': '172.19.3.71',
    'port': 1433,
    'database': 'pop6768',
    'username': 'danny',
    'password': 'P@ssw0rd12#$'
}

# รายชื่อ ODBC Drivers ที่จะลองใช้เชื่อมต่อ (เรียงตามลำดับความใหม่)
POSSIBLE_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "SQL Server Native Client 11.0",
    "SQL Server"
]

# --- ตั้งค่าตาราง ---
SOURCE_TABLE = 'random_test'
DESTINATION_TABLE = 'random_test_result'

# --- ตั้งค่าการสุ่มสำหรับ "ครัวเรือนกลุ่มบุคคล" ---
SAMPLING_INTERVAL = 5

# เงื่อนไขการกรองข้อมูลจาก SOURCE_TABLE
# ใช้ List สำหรับเงื่อนไข IN ('2', '3')
SAMPLING_FILTERS = {
    # 'HouseholdType': ['2', '3']
    'HouseholdType': ['2']
}

# คอลัมน์ที่ใช้เรียงลำดับก่อนการสุ่ม
HOUSEHOLD_ORDERING_COLUMN = 'sort_number'

# ขอบเขตของการสุ่ม (ระบุคอลัมน์ที่ใช้ในการจับกลุ่ม)
# <-- กำหนดให้จับกลุ่มตาม RegCode และ AreaCode ที่นี่
PARTITION_BY_COLUMNS = ['RegCode', 'AreaCode']

# ค่าที่จะบันทึกในฟิลด์ random_type
RANDOM_TYPE_VALUE = 'ครัวเรือนกลุ่มบุคคล'
