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
USE pop6768;
GO

WITH NumberedRows AS (
    SELECT
        sort_number,
        ROW_NUMBER() OVER (
            PARTITION BY RegCode, AreaCode
            
            ORDER BY
                ProvCode, 
                DistCode,
                SubDistCode,
                EA_No,
                HouseholdNumber
        ) AS rn
    FROM
        random_test
    WHERE
        HouseholdType = '2'
)
UPDATE NumberedRows
SET sort_number = rn;
GO
```