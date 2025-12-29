# CSV File Analysis Summary

## Overview
Analysis of CSV files in `DATA_FILES_TO_IMPORT` directory against table definitions in `CreateStatements.sql`

## Critical Issues

### 1. Missing Table Definition
- **File**: `SIDTA_F03012_20251119_142038_20251119_145536.csv`
- **Issue**: Table `F03012` does not exist in `CreateStatements.sql`
- **Action Required**: Either create the table definition or remove/rename the CSV file

## Empty Files (No Data Rows)

The following files are empty (contain no data rows, only headers or completely empty):

1. `SICTL_F0005_20251119_142038_20251119_145812.csv` - F0005 (0 bytes)
2. `SIDTA_F0006_20251119_142038_20251119_145812.csv` - F0006 (0 bytes)
3. `SIDTA_F01151_20251119_142038_20251119_145812.csv` - F01151 (0 bytes)
4. `SIDTA_F0116_20251119_142038_20251119_145812.csv` - F0116 (0 bytes)
5. `SIDTA_F0150_20251119_142038_20251119_145812.csv` - F0150 (0 bytes)
6. `SIDTA_F03B11_20251119_142038_20251119_145812.csv` - F03B11 (0 bytes)
7. `SIDTA_F41002_20251119_142038_20251119_145812.csv` - F41002 (0 bytes)
8. `SIDTA_F4101_20251119_142038_20251119_145812.csv` - F4101 (0 bytes)
9. `SIDTA_F4106_20251119_142038_20251119_145812.csv` - F4106 (0 bytes)
10. `SIDTA_F4201_20251119_142038_20251119_145812.csv` - F4201 (0 bytes)
11. `SIDTA_F4211_20251119_142038_20251119_145812.csv` - F4211 (0 bytes)
12. `SIDTA_F42119_20251119_142038_20251119_145812.csv` - F42119 (0 bytes)

**Total**: 12 empty files

## Files with Partial Column Data

The following files contain data but only include a subset of table columns (this is expected behavior - only populated columns are exported):

### F40942
- **CSV Columns**: 3 (CKCPGP, CKCGP1, CKCGID)
- **Table Columns**: 12
- **Missing**: CKCGP2-10 (will be set to NULL)

### F0101
- **CSV Columns**: 12
- **Table Columns**: 95
- **Missing**: 83 columns (will be set to NULL)

### F0111
- **CSV Columns**: 3 (WWAN8, WWIDLN, WWMLNM)
- **Table Columns**: 53
- **Missing**: 50 columns (will be set to NULL)

### F0115
- **CSV Columns**: 5 (WPAN8, WPIDLN, WPPHTP, WPPH1, WPRCK7)
- **Table Columns**: 17
- **Missing**: 12 columns (will be set to NULL)

### F4008
- **CSV Columns**: 4 (TATXA1, TATXR1, TAEFTJ, TAEFDJ)
- **Table Columns**: 35
- **Missing**: 31 columns (will be set to NULL)

### F4102
- **CSV Columns**: 9
- **Table Columns**: 170
- **Missing**: 161 columns (will be set to NULL)

### F41021
- **CSV Columns**: 6 (LIMCU, LIITM, LIPQOH, LIHCOM, LIPCOM, LIQOWO)
- **Table Columns**: 54
- **Missing**: 48 columns (will be set to NULL)

### F4104
- **CSV Columns**: 6 (IVXRT, IVLITM, IVCITM, IVEFTJ, IVEXDJ, IVAN8)
- **Table Columns**: 30
- **Missing**: 24 columns (will be set to NULL)

## Files Ready for Import

The following files have data and can be imported:

1. `SICTL_F40942_20251119_142038_20251119_145536.csv` - 53 rows
2. `SIDTA_F0101_20251119_142038_20251119_145536.csv` - 1 row
3. `SIDTA_F0111_20251119_142038_20251119_145536.csv` - 1 row
4. `SIDTA_F0115_20251119_142038_20251119_145536.csv` - 1 row
5. `SIDTA_F4008_20251119_142038_20251119_145536.csv` - 22 rows
6. `SIDTA_F4102_20251119_142038_20251119_145536.csv` - 1 row
7. `SIDTA_F41021_20251119_142038.csv` - 2 rows
8. `SIDTA_F4104_20251119_142038_20251119_145536.csv` - 2 rows

**Total**: 8 files with data (83 total rows)

## Import Script Usage

The `Import-CsvToSql.ps1` script will:
- Check if CSV files are empty before processing
- Map CSV columns to table columns (only import columns that exist in CSV)
- Set missing columns to NULL (if nullable)
- Show progress on screen
- Log all operations to `CSV_IMPORT_LOG` table
- Handle errors gracefully

### Parameters:
- `-CsvDirectory`: Directory containing CSV files (default: "DATA_FILES_TO_IMPORT")
- `-ServerInstance`: SQL Server instance (default: "jdesql01")
- `-Database`: Database name (default: "JDE_PRODUCTION")
- `-Username`: SQL Server username (default: "sa")
- `-Password`: SQL Server password (default: "Pcare2009")
- `-IntegratedSecurity`: Use Windows Authentication
- `-BatchSize`: Batch size for bulk operations (default: 1000)
- `-TruncateBeforeImport`: Truncate table before importing (use with caution!)

### Example:
```powershell
.\Import-CsvToSql.ps1 -ServerInstance "jdesql01" -Database "JDE_PRODUCTION" -Username "sa" -Password "Pcare2009"
```

## Log Table Structure

The log table `CSV_IMPORT_LOG` tracks:
- Import start/end times
- File name and target table
- Status (SUCCESS, FAILED, SKIPPED)
- Row counts (processed, inserted, failed)
- Error messages
- Duration
- Column mapping information

Query recent imports:
```sql
SELECT TOP 20 * 
FROM [DBO].[CSV_IMPORT_LOG] 
ORDER BY [StartTime] DESC
```

