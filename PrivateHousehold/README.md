ติดตั้ง package library
```bash
pip install -r requirements.txt
```

รันไฟล์ main.py
```bash
python main.py
```

SQL Command
```bash
-- เรียงลำดับตามลำดับครัวเรือน
USE pop6768;
GO

WITH NumberedRows AS (
    SELECT
        sort_number,
        ROW_NUMBER() OVER (
            PARTITION BY RegCode, AreaCode, EA_No
            ORDER BY HouseholdNumber
        ) AS rn
    FROM
        random_test
    WHERE
        HouseholdType = '1'
)
UPDATE NumberedRows
SET sort_number = rn;
GO
```