DB_CONFIG = {
    'server': '172.19.3.71',
    'port': 1433,
    'database': 'pop6768',
    'username': 'danny',
    'password': 'P@ssw0rd12#$'
}

POSSIBLE_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "SQL Server Native Client 11.0",
    "SQL Server"
]

SAMPLING_INTERVAL = 5 # ช่วงการสุ่ม (I)

SOURCE_TABLE = 'random_test'
DESTINATION_TABLE = 'random_test_result'
HOUSEHOLD_ORDERING_COLUMN = 'sort_number'

RANDOM_TYPE_VALUE = 'ครัวเรือนส่วนบุคคล'

SAMPLING_FILTERS = {
    'HouseholdType': '1'
}