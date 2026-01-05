param(
    [string]$CsvDirectory = "C:\Users\shedger\Documents\code\JDE92-Master-Data\DATA_FILES_TO_IMPORT",
    [string]$ServerInstance = 'localhost\SQLExpress',
    [string]$Database = 'sales-dashboard',
    [string]$Username = 'sa',
    [string]$Password = 'Pcare2009',
    [switch]$IntegratedSecurity,
    [bool]$TruncateBeforeImport = $true,
    [string]$TempFolder = "C:\Users\shedger\Documents\code\JDE92-Master-Data\DATA_FILES_TO_IMPORT",
    [hashtable]$TruncateTableForFiles = @{
        "SIDTA_F4008" = $true
        "SIDTA_F40942" = $true
    }
)

function Import-CsvToSql {
    param(
        [string]$CsvDirectory = "C:\Users\shedger\Documents\code\JDE92-Master-Data\DATA_FILES_TO_IMPORT",
        [string]$ServerInstance = 'localhost\SQLExpress',
        [string]$Database = 'sales-dashboard',
        [string]$Username = 'sa',
        [string]$Password = 'Pcare2009',
        [switch]$IntegratedSecurity,
        [bool]$TruncateBeforeImport = $true,
        [string]$TempFolder = "C:\Users\shedger\Documents\code\JDE92-Master-Data\DATA_FILES_TO_IMPORT",
        [hashtable]$TruncateTableForFiles = @{
            "SIDTA_F4008" = $true
            "SIDTA_F40942" = $true
        }
    )
    
    $ErrorActionPreference = "Stop"
    
    #$logTableName = "[DBO].[CSV_IMPORT_LOG]"
    $startTime = Get-Date
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    
    if ($PSScriptRoot) {
        $scriptPath = $PSScriptRoot
    } elseif ($MyInvocation.PSScriptRoot) {
        $scriptPath = $MyInvocation.PSScriptRoot
    } else {
        $scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
    }
    $logFilePath = Join-Path $scriptPath "${timestamp}_import_run.log"
    
    function Write-Log {
        param(
            [string]$Message,
            [string]$Level = "INFO"
        )
        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        $color = switch ($Level) {
            "ERROR" { "Red" }
            "WARNING" { "Yellow" }
            "SUCCESS" { "Green" }
            default { "White" }
        }
        Write-Host "[$timestamp] [$Level] $Message" -ForegroundColor $color
    }
    
    function Write-LogToFile {
        param(
            [string]$Message,
            [string]$Level = "INFO"
        )
        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        $logMessage = "[$timestamp] [$Level] $Message"
        try {
            Add-Content -Path $logFilePath -Value $logMessage -Encoding UTF8 -ErrorAction SilentlyContinue
        } catch {
        }
    }
    
    function Test-DatabaseConnection {
        param(
            [string]$ConnectionString,
            [string]$ServerInstance,
            [string]$Database
        )
        
        Write-Log "Step 1: Creating connection object..." "INFO"
        Write-LogToFile "Testing database connection to Server: $ServerInstance, Database: $Database" "INFO"
        
        $testConnection = $null
        try {
            Write-Log "Step 2: Opening connection to $ServerInstance..." "INFO"
            $testConnection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
            $testConnection.Open()
            
            Write-Log "Step 3: Executing test query (SELECT @@VERSION)..." "INFO"
            #$testQuery = "SELECT @@VERSION"
            #$testCommand = New-Object System.Data.SqlClient.SqlCommand($testQuery, $testConnection)
            #$version = $testCommand.ExecuteScalar()
            
            Write-Log "Step 4: Connection test successful!" "SUCCESS"
            Write-Log "SQL Server version detected" "SUCCESS"
            Write-LogToFile "Database connection test successful" "SUCCESS"
            return $true
        } catch {
            Write-Log "Step 4: Connection test FAILED" "ERROR"
            $errorMessage = "Database connection test failed: $($_.Exception.Message)"
            Write-Log $errorMessage "ERROR"
            Write-LogToFile $errorMessage "ERROR"
            
            if ($_.Exception.InnerException) {
                $innerError = "Inner exception: $($_.Exception.InnerException.Message)"
                Write-Log $innerError "ERROR"
                Write-LogToFile $innerError "ERROR"
            }
            
            return $false
        } finally {
            if ($null -ne $testConnection -and $testConnection.State -eq "Open") {
                Write-Log "Closing test connection..." "INFO"
                $testConnection.Close()
                $testConnection.Dispose()
            }
        }
    }
    
    function Initialize-ProcessingFolders {
        param(
            [string]$BaseDirectory
        )
        
        $processedFolder = Join-Path $BaseDirectory "Processed"
        $errorFolder = Join-Path $BaseDirectory "Error"
        
        if (-not (Test-Path $processedFolder)) {
            New-Item -ItemType Directory -Path $processedFolder -Force | Out-Null
            Write-Log "Created Processed folder: $processedFolder" "SUCCESS"
        }
        
        if (-not (Test-Path $errorFolder)) {
            New-Item -ItemType Directory -Path $errorFolder -Force | Out-Null
            Write-Log "Created Error folder: $errorFolder" "SUCCESS"
        }
        
        return @{
            Processed = $processedFolder
            Error = $errorFolder
        }
    }
    
    function Move-CsvFile {
        param(
            [System.IO.FileInfo]$CsvFile,
            [string]$DestinationFolder,
            [string]$Status
        )
        
        try {
            $destinationPath = Join-Path $DestinationFolder $CsvFile.Name
            
            if (Test-Path $destinationPath) {
                #$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
                $nameWithoutExt = [System.IO.Path]::GetFileNameWithoutExtension($CsvFile.Name)
                $extension = $CsvFile.Extension
                #$destinationPath = Join-Path $DestinationFolder "$nameWithoutExt`_$timestamp$extension"
                $destinationPath = Join-Path $DestinationFolder "$nameWithoutExt$extension"
            }
            
            Move-Item -Path $CsvFile.FullName -Destination $destinationPath -Force
            Write-Log "Moved file to $Status folder: $($CsvFile.Name)" "SUCCESS"
            return $true
        } catch {
            Write-Log "Failed to move file $($CsvFile.Name) to $Status folder: $_" "ERROR"
            return $false
        }
    }
    
    function New-LogTable {
        param(
            [System.Data.SqlClient.SqlConnection]$Connection
        )
        
        $checkTableQuery = @"
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CSV_IMPORT_LOG' AND schema_id = SCHEMA_ID('DBO'))
BEGIN
    CREATE TABLE [DBO].[CSV_IMPORT_LOG] (
        [LogID] bigint IDENTITY(1,1) NOT NULL,
        [StartTime] datetime2 NOT NULL,
        [EndTime] datetime2 NULL,
        [FileName] nvarchar(500) NOT NULL,
        [TableName] nvarchar(128) NOT NULL,
        [Status] nvarchar(20) NOT NULL,
        [RowsProcessed] int NULL,
        [RowsInserted] int NULL,
        [RowsFailed] int NULL,
        [BatchSize] int NULL,
        [ErrorMessage] nvarchar(max) NULL,
        [DurationSeconds] float NULL,
        [CsvColumnCount] int NULL,
        [TableColumnCount] int NULL,
        [MissingColumns] nvarchar(max) NULL,
        CONSTRAINT [CSV_IMPORT_LOG_PK] PRIMARY KEY ([LogID])
    );
    
    CREATE INDEX [IX_CSV_IMPORT_LOG_FileName] ON [DBO].[CSV_IMPORT_LOG] ([FileName]);
    CREATE INDEX [IX_CSV_IMPORT_LOG_TableName] ON [DBO].[CSV_IMPORT_LOG] ([TableName]);
    CREATE INDEX [IX_CSV_IMPORT_LOG_StartTime] ON [DBO].[CSV_IMPORT_LOG] ([StartTime]);
END
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($checkTableQuery, $Connection)
        $command.ExecuteNonQuery() | Out-Null
        Write-Log "Log table ensured" "SUCCESS"
    }
    
    function Get-TableColumns {
        param(
            [System.Data.SqlClient.SqlConnection]$Connection,
            [string]$TableName
        )
        
        $query = @"
SELECT 
    COLUMN_NAME AS ColumnName,
    DATA_TYPE AS DataType,
    IS_NULLABLE AS IsNullable,
    CHARACTER_MAXIMUM_LENGTH AS CharLength
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'DBO' AND TABLE_NAME = @TableName
ORDER BY ORDINAL_POSITION
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $Connection)
        $command.Parameters.AddWithValue("@TableName", $TableName) | Out-Null
        $reader = $command.ExecuteReader()
        
        $columns = @()
        while ($reader.Read()) {
            $charLength = $reader["CharLength"]
            if ($charLength -is [DBNull]) {
                $charLength = $null
            } else {
                $charLength = [int]$charLength
            }
            
            $columns += [PSCustomObject]@{
                ColumnName = $reader["ColumnName"].ToString()
                DataType = $reader["DataType"].ToString()
                IsNullable = $reader["IsNullable"].ToString() -eq "YES"
                CharLength = $charLength
            }
        }
        $reader.Close()
        
        return $columns
    }
    
    function Invoke-SqlBulkInsert {
        param(
            [System.Data.SqlClient.SqlConnection]$Connection,
            [System.IO.FileInfo]$CsvFile,
            [string]$TableName,
            [array]$MatchingColumns,
            [hashtable]$TableColumnInfo,
            [string]$TempFolder
        )
        
        $tempTableName = "##TempImport_$([System.Guid]::NewGuid().ToString().Replace('-', ''))"
        
        try {
            Write-Log "Reading CSV data into memory" "INFO"
            $csvData = Import-Csv -Path $CsvFile.FullName -Encoding UTF8
            
            Write-Log "Creating temporary staging table: $tempTableName" "INFO"
            
            $createTempTableQuery = "CREATE TABLE [$tempTableName] ("
            $columns = @()
            foreach ($colName in $MatchingColumns) {
                $dataType = $TableColumnInfo[$colName].DataType
                $charLength = $TableColumnInfo[$colName].CharLength
                $isNullable = $TableColumnInfo[$colName].IsNullable
                
                $sqlType = switch -Regex ($dataType) {
                    '^(n?char)' {
                        if ($null -ne $charLength) {
                            "$dataType($charLength)"
                        } else {
                            "$dataType(255)"
                        }
                    }
                    '^(n?varchar)' {
                        if ($null -ne $charLength) {
                            "$dataType($charLength)"
                        } else {
                            "$dataType(MAX)"
                        }
                    }
                    default { $dataType }
                }
                
                $nullability = if ($isNullable) { "NULL" } else { "NOT NULL" }
                $columns += "[$colName] $sqlType $nullability"
            }
            $createTempTableQuery += ($columns -join ", ") + ")"
            
            $command = New-Object System.Data.SqlClient.SqlCommand($createTempTableQuery, $Connection)
            $command.ExecuteNonQuery() | Out-Null
            
            Write-Log "Loading data into staging table using SqlBulkCopy" "INFO"
            
            $bulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy($Connection)
            $bulkCopy.DestinationTableName = "[$tempTableName]"
            $bulkCopy.BatchSize = 1000
            $bulkCopy.BulkCopyTimeout = 300
            
            $dataTable = New-Object System.Data.DataTable
            foreach ($colName in $MatchingColumns) {
                $dataType = $TableColumnInfo[$colName].DataType
                $netType = switch -Regex ($dataType) {
                    '^int$' { [System.Int32] }
                    '^bigint$' { [System.Int64] }
                    '^smallint$' { [System.Int16] }
                    '^tinyint$' { [System.Byte] }
                    '^bit$' { [System.Boolean] }
                    '^decimal|numeric' { [System.Decimal] }
                    '^float$' { [System.Double] }
                    '^real$' { [System.Single] }
                    '^money$' { [System.Decimal] }
                    '^smallmoney$' { [System.Decimal] }
                    '^date$' { [System.DateTime] }
                    '^datetime$' { [System.DateTime] }
                    '^datetime2$' { [System.DateTime] }
                    '^smalldatetime$' { [System.DateTime] }
                    '^time$' { [System.TimeSpan] }
                    default { [System.String] }
                }
                $dataTable.Columns.Add($colName, $netType) | Out-Null
                $bulkCopy.ColumnMappings.Add($colName, $colName) | Out-Null
            }
            
            $rowCount = 0
            $skippedRows = 0
            foreach ($row in $csvData) {
                $dataRow = $dataTable.NewRow()
                $skipRow = $false
                
                foreach ($colName in $MatchingColumns) {
                    $val = $row.$colName
                    $isNullable = $TableColumnInfo[$colName].IsNullable
                    $dataType = $TableColumnInfo[$colName].DataType
                    $netType = $dataTable.Columns[$colName].DataType
                    
                    if ([string]::IsNullOrWhiteSpace($val)) {
                        if ($isNullable) {
                            $dataRow[$colName] = [DBNull]::Value
                        } else {
                            $defaultValue = switch -Regex ($dataType) {
                                '^int$' { 0 }
                                '^bigint$' { [long]0 }
                                '^smallint$' { [short]0 }
                                '^tinyint$' { [byte]0 }
                                '^bit$' { $false }
                                '^decimal|numeric' { [decimal]0 }
                                '^float$' { [double]0 }
                                '^real$' { [float]0 }
                                '^money$|^smallmoney$' { [decimal]0 }
                                '^date$|^datetime$|^datetime2$|^smalldatetime$' { [System.DateTime]::MinValue }
                                default { "" }
                            }
                            $dataRow[$colName] = $defaultValue
                        }
                    } else {
                        try {
                            if ($netType -eq [System.DateTime]) {
                                $parsedDate = [System.DateTime]::MinValue
                                if ([System.DateTime]::TryParse($val, [ref]$parsedDate)) {
                                    $dataRow[$colName] = $parsedDate
                                } else {
                                    if ($isNullable) {
                                        $dataRow[$colName] = [DBNull]::Value
                                    } else {
                                        $dataRow[$colName] = [System.DateTime]::MinValue
                                    }
                                }
                            } elseif ($netType -eq [System.Boolean]) {
                                $boolVal = $false
                                if ([bool]::TryParse($val, [ref]$boolVal)) {
                                    $dataRow[$colName] = $boolVal
                                } else {
                                    $dataRow[$colName] = ($val -ne "0" -and $val -ne "" -and $val -ne "false")
                                }
                            } elseif ($netType.IsValueType -and $netType -ne [System.String]) {
                                $dataRow[$colName] = [Convert]::ChangeType($val, $netType)
                            } else {
                                $dataRow[$colName] = $val
                            }
                        } catch {
                            $errorMsg = $_.Exception.Message
                            Write-Log "Warning: Could not convert value '$val' for column $colName (type: $dataType) in row ${rowCount}: $errorMsg" "WARNING"
                            if ($isNullable) {
                                $dataRow[$colName] = [DBNull]::Value
                            } else {
                                $skipRow = $true
                                break
                            }
                        }
                    }
                }
                
                if (-not $skipRow) {
                    $dataTable.Rows.Add($dataRow)
                    $rowCount++
                    
                    if ($rowCount % 1000 -eq 0) {
                        $bulkCopy.WriteToServer($dataTable)
                        $dataTable.Clear()
                        Write-Log "Loaded $rowCount rows..." "INFO"
                    }
                } else {
                    $skippedRows++
                }
            }
            
            if ($skippedRows -gt 0) {
                Write-Log "Skipped $skippedRows rows due to data validation errors" "WARNING"
            }
            
            if ($dataTable.Rows.Count -gt 0) {
                $bulkCopy.WriteToServer($dataTable)
            }
            
            $bulkCopy.Close()
            
            Write-Log "Copying data from staging table to target table (deduplicating)" "INFO"
            
            $columnList = ($MatchingColumns | ForEach-Object { "[$_]" }) -join ", "
            $insertQuery = @"
INSERT INTO [DBO].[$TableName] ($columnList) 
SELECT $columnList 
FROM (
    SELECT $columnList, ROW_NUMBER() OVER (PARTITION BY $columnList ORDER BY (SELECT NULL)) AS rn
    FROM [$tempTableName]
) AS deduped
WHERE rn = 1
"@
            
            $command = New-Object System.Data.SqlClient.SqlCommand($insertQuery, $Connection)
            $rowsInserted = $command.ExecuteNonQuery()
            
            Write-Log "Dropping temporary staging table" "INFO"
            $dropQuery = "DROP TABLE [$tempTableName]"
            $command = New-Object System.Data.SqlClient.SqlCommand($dropQuery, $Connection)
            $command.ExecuteNonQuery() | Out-Null
            
            return $rowsInserted
        } catch {
            try {
                if ($null -ne $bulkCopy) {
                    $bulkCopy.Close()
                }
                $dropQuery = "IF OBJECT_ID('tempdb..[$tempTableName]') IS NOT NULL DROP TABLE [$tempTableName]"
                $command = New-Object System.Data.SqlClient.SqlCommand($dropQuery, $Connection)
                $command.ExecuteNonQuery() | Out-Null
            } catch {
                Write-Log "Warning: Could not drop temporary table $tempTableName" "WARNING"
            }
            throw
        }
    }
    
    function Write-LogEntry {
        param(
            [System.Data.SqlClient.SqlConnection]$Connection,
            [string]$FileName,
            [string]$TableName,
            [string]$Status,
            [int]$RowsProcessed = 0,
            [int]$RowsInserted = 0,
            [int]$RowsFailed = 0,
            [int]$BatchSize = 0,
            [string]$ErrorMessage = $null,
            [float]$DurationSeconds = 0,
            [int]$CsvColumnCount = 0,
            [int]$TableColumnCount = 0,
            [string]$MissingColumns = $null
        )
        
        $query = @"
INSERT INTO [DBO].[CSV_IMPORT_LOG] 
    ([StartTime], [EndTime], [FileName], [TableName], [Status], [RowsProcessed], [RowsInserted], [RowsFailed], 
     [BatchSize], [ErrorMessage], [DurationSeconds], [CsvColumnCount], [TableColumnCount], [MissingColumns])
VALUES 
    (@StartTime, @EndTime, @FileName, @TableName, @Status, @RowsProcessed, @RowsInserted, @RowsFailed,
     @BatchSize, @ErrorMessage, @DurationSeconds, @CsvColumnCount, @TableColumnCount, @MissingColumns)
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $Connection)
        $command.Parameters.AddWithValue("@StartTime", $script:fileStartTime) | Out-Null
        $command.Parameters.AddWithValue("@EndTime", (Get-Date)) | Out-Null
        $command.Parameters.AddWithValue("@FileName", $FileName) | Out-Null
        $command.Parameters.AddWithValue("@TableName", $TableName) | Out-Null
        $command.Parameters.AddWithValue("@Status", $Status) | Out-Null
        $command.Parameters.AddWithValue("@RowsProcessed", $RowsProcessed) | Out-Null
        $command.Parameters.AddWithValue("@RowsInserted", $RowsInserted) | Out-Null
        $command.Parameters.AddWithValue("@RowsFailed", $RowsFailed) | Out-Null
        $command.Parameters.AddWithValue("@BatchSize", $BatchSize) | Out-Null
        $command.Parameters.AddWithValue("@DurationSeconds", $DurationSeconds) | Out-Null
        $command.Parameters.AddWithValue("@CsvColumnCount", $CsvColumnCount) | Out-Null
        $command.Parameters.AddWithValue("@TableColumnCount", $TableColumnCount) | Out-Null
        
        if ($null -eq $ErrorMessage) {
            $command.Parameters.AddWithValue("@ErrorMessage", [DBNull]::Value) | Out-Null
        } else {
            $command.Parameters.AddWithValue("@ErrorMessage", $ErrorMessage) | Out-Null
        }
        
        if ($null -eq $MissingColumns) {
            $command.Parameters.AddWithValue("@MissingColumns", [DBNull]::Value) | Out-Null
        } else {
            $command.Parameters.AddWithValue("@MissingColumns", $MissingColumns) | Out-Null
        }
        
        $command.ExecuteNonQuery() | Out-Null
    }
    
    function Import-CsvFile {
        param(
            [System.Data.SqlClient.SqlConnection]$Connection,
            [System.IO.FileInfo]$CsvFile,
            [string]$TableName,
            [string]$ProcessedFolder,
            [string]$ErrorFolder,
            [string]$SourceFolder,
            [switch]$TruncateBeforeImport,
            [string]$TempFolder,
            [hashtable]$TruncateTableForFiles = @{}
        )
        
        $script:fileStartTime = Get-Date
        $fileName = $CsvFile.Name
        $fileProcessedSuccessfully = $false
        
        Write-Host "Processing: $fileName" -ForegroundColor White
        Write-Host "Target table: $TableName" -ForegroundColor White
        Write-Log "Processing file: $fileName -> Table: $TableName"
        
        try {
            Write-Host "Step 1: Checking file path..." -ForegroundColor Gray
            Write-Log "Step 1: Checking file path" "INFO"
            if (-not (Test-Path $CsvFile.FullName)) {
                throw "File not found: $($CsvFile.FullName)"
            }
            
            Write-Host "Step 2: Checking file size..." -ForegroundColor Gray
            Write-Log "Step 2: Checking file size" "INFO"
            if ($CsvFile.Length -eq 0) {
                Write-Host "File is empty, skipping..." -ForegroundColor Yellow
                Write-Log "File is empty, treating as success" "WARNING"
                Write-LogEntry -Connection $Connection -FileName $fileName -TableName $TableName -Status "SUCCESS" -DurationSeconds 0 -CsvColumnCount 0 -TableColumnCount 0 -ErrorMessage "File is empty"
                $fileProcessedSuccessfully = $true
                Move-CsvFile -CsvFile $CsvFile -DestinationFolder $ProcessedFolder -Status "Processed" | Out-Null
                return $fileProcessedSuccessfully
            }
            
            Write-Host "Step 3: Reading CSV file..." -ForegroundColor Gray
            Write-Log "Step 3: Importing CSV" "INFO"
            try {
                $csvData = Import-Csv -Path $CsvFile.FullName -Encoding UTF8 -ErrorAction Stop
                Write-Host "CSV file read successfully" -ForegroundColor Green
            } catch {
                throw "Failed to import CSV file '$fileName': $_"
            }
            
            if ($null -eq $csvData) {
                Write-Log "CSV file returned null data, treating as success" "WARNING"
                Write-LogEntry -Connection $Connection -FileName $fileName -TableName $TableName -Status "SUCCESS" -DurationSeconds 0 -CsvColumnCount 0 -TableColumnCount 0 -ErrorMessage "CSV data is null"
                $fileProcessedSuccessfully = $true
                Move-CsvFile -CsvFile $CsvFile -DestinationFolder $ProcessedFolder -Status "Processed" | Out-Null
                return $fileProcessedSuccessfully
            }
            
            Write-Log "Step 4: Converting to array" "INFO"
            $csvDataArray = @($csvData)
            
            if ($null -eq $csvDataArray) {
                throw "Failed to convert CSV data to array for file '$fileName'"
            }
            
            Write-Host "Step 5: Validating data rows (Found: $($csvDataArray.Count) rows)..." -ForegroundColor Gray
            Write-Log "Step 5: Checking array count (Count: $($csvDataArray.Count))" "INFO"
            if ($csvDataArray.Count -eq 0) {
                Write-Host "File contains only header row, skipping..." -ForegroundColor Yellow
                Write-Log "File contains only header row, treating as success" "WARNING"
                try {
                    $csvColumns = ($csvData | Get-Member -MemberType NoteProperty).Name
                } catch {
                    $csvColumns = @()
                }
                Write-LogEntry -Connection $Connection -FileName $fileName -TableName $TableName -Status "SUCCESS" -DurationSeconds 0 -CsvColumnCount $csvColumns.Count -TableColumnCount 0 -ErrorMessage "No data rows"
                $fileProcessedSuccessfully = $true
                Move-CsvFile -CsvFile $CsvFile -DestinationFolder $ProcessedFolder -Status "Processed" | Out-Null
                return $fileProcessedSuccessfully
            }
            
            Write-Log "Step 6: Checking first row" "INFO"
            if ($csvDataArray.Count -gt 0) {
                if ($null -eq $csvDataArray[0]) {
                    Write-Log "CSV file first row is null, treating as success" "WARNING"
                    Write-LogEntry -Connection $Connection -FileName $fileName -TableName $TableName -Status "SUCCESS" -DurationSeconds 0 -CsvColumnCount 0 -TableColumnCount 0 -ErrorMessage "First CSV row is null"
                    $fileProcessedSuccessfully = $true
                    Move-CsvFile -CsvFile $CsvFile -DestinationFolder $ProcessedFolder -Status "Processed" | Out-Null
                    return $fileProcessedSuccessfully
                }
            }
            
            Write-Host "Step 7: Analyzing CSV columns..." -ForegroundColor Gray
            Write-Log "Step 7: Getting CSV columns" "INFO"
            try {
                $csvColumns = ($csvDataArray[0] | Get-Member -MemberType NoteProperty).Name
                Write-Host "Found $($csvColumns.Count) columns in CSV" -ForegroundColor Green
            } catch {
                throw "Failed to get column names from CSV file '$fileName': $_"
            }
            
            if ($null -eq $csvColumns -or $csvColumns.Count -eq 0) {
                throw "Could not determine CSV column names from file '$fileName'"
            }
            
            Write-Host "Step 8: Retrieving table structure..." -ForegroundColor Gray
            Write-Log "Step 8: Getting table columns" "INFO"
            $tableColumns = Get-TableColumns -Connection $Connection -TableName $TableName
            Write-Host "Found $($tableColumns.Count) columns in table" -ForegroundColor Green
            
            Write-Log "Step 9: Validating table columns" "INFO"
            if ($null -eq $tableColumns -or $tableColumns.Count -eq 0) {
                throw "Table '$TableName' does not exist in database or has no columns"
            }
            
            Write-Log "Step 10: Validating columns and checking required fields" "INFO"
            Write-Log "Found $($tableColumns.Count) columns in table" "INFO"
            
            $tableColumnInfo = @{}
            $requiredColumns = @()
            
            foreach ($col in $tableColumns) {
                $colName = $col.ColumnName
                $isNullable = $col.IsNullable
                
                $tableColumnInfo[$colName] = @{
                    IsNullable = $isNullable
                    DataType = $col.DataType
                    CharLength = $col.CharLength
                }
                
                if (-not $isNullable) {
                    $requiredColumns += $colName
                }
            }
            
            $matchingColumns = @($csvColumns | Where-Object { $tableColumnInfo.ContainsKey($_) })
            $missingColumns = @($tableColumnInfo.Keys | Where-Object { $csvColumns -notcontains $_ })
            $missingRequiredColumns = @($requiredColumns | Where-Object { $csvColumns -notcontains $_ })
            
            if ($matchingColumns.Count -eq 0) {
                throw "No matching columns found between CSV and table"
            }
            
            Write-Host "Column matching: CSV=$($csvColumns.Count), Table=$($tableColumnInfo.Count), Matching=$($matchingColumns.Count)" -ForegroundColor Cyan
            Write-Log "CSV columns: $($csvColumns.Count), Table columns: $($tableColumnInfo.Count), Matching: $($matchingColumns.Count)"
            
            if ($missingRequiredColumns.Count -gt 0) {
                $csvColList = $csvColumns -join ', '
                $missingReqColList = $missingRequiredColumns -join ', '
                throw "Missing required (non-nullable) columns in CSV file '$fileName' for table '$TableName'. CSV contains columns: [$csvColList]. Missing required columns: [$missingReqColList]"
            }
            
            if ($missingColumns.Count -gt 0) {
                Write-Log "Missing optional columns (will be set to NULL): $($missingColumns -join ', ')" "WARNING"
            }
            
            Write-Host "Step 11: Preparing bulk insert..." -ForegroundColor Gray
            Write-Log "Step 11: Preparing bulk insert" "INFO"
            
            $shouldTruncate = $false
            $matchingKey = $TruncateTableForFiles.Keys | Where-Object { $fileName -like "*$_*" } | Select-Object -First 1
            if ($matchingKey) {
                $shouldTruncate = $TruncateTableForFiles[$matchingKey]
                Write-Host "Hash table match: '$fileName' -> key '$matchingKey' = $shouldTruncate" -ForegroundColor Cyan
                Write-Log "File '$fileName' matched hash table key '$matchingKey'. Truncate setting: $shouldTruncate" "INFO"
            } elseif ($TruncateBeforeImport) {
                $shouldTruncate = $true
                Write-Host "No hash table match. Using global TruncateBeforeImport = $shouldTruncate" -ForegroundColor Yellow
                Write-Log "File '$fileName' not in hash table. Using global TruncateBeforeImport: $shouldTruncate" "INFO"
            } else {
                Write-Host "No hash table match and TruncateBeforeImport = false. Will not truncate." -ForegroundColor Gray
                Write-Log "File '$fileName' not in hash table and TruncateBeforeImport is false. Will not truncate." "INFO"
            }
            
            if ($shouldTruncate) {
                Write-Host "Truncating table before import..." -ForegroundColor Yellow
                Write-Log "Truncating table before import..." "WARNING"
                $truncateQuery = "TRUNCATE TABLE [DBO].[$TableName]"
                $command = New-Object System.Data.SqlClient.SqlCommand($truncateQuery, $Connection)
                $command.ExecuteNonQuery() | Out-Null
                Write-Host "Table truncated successfully" -ForegroundColor Green
                Write-Log "Table truncated successfully" "SUCCESS"
            }
            
            $rowsProcessed = $csvDataArray.Count
            $rowsInserted = 0
            $rowsFailed = 0
            
            Write-Host ""
            Write-Host "Step 12: Executing bulk insert ($rowsProcessed rows)..." -ForegroundColor Yellow
            Write-Host "This may take a moment..." -ForegroundColor Gray
            try {
                Write-Log "Step 12: Executing SQL Server BULK INSERT ($rowsProcessed rows)" "INFO"
                $rowsInserted = Invoke-SqlBulkInsert -Connection $Connection -CsvFile $CsvFile -TableName $TableName -MatchingColumns $matchingColumns -TableColumnInfo $tableColumnInfo -TempFolder $TempFolder
                Write-Host "Bulk insert completed: $rowsInserted rows inserted" -ForegroundColor Green
                Write-Log "Bulk insert completed: $rowsInserted rows inserted" "SUCCESS"
            } catch {
                $rowsFailed = $rowsProcessed - $rowsInserted
                Write-Log "Bulk insert failed: $_" "ERROR"
                throw
            }
            
            $duration = ((Get-Date) - $script:fileStartTime).TotalSeconds
            
            if ($rowsInserted -eq 0) {
                if ($rowsFailed -gt 0) {
                    throw "Import failed: No rows were inserted. $rowsFailed rows failed to process."
                } else {
                    throw "Import failed: No rows were inserted"
                }
            }
            
            if ($rowsFailed -gt 0) {
                Write-Log "Import completed with errors: $rowsInserted inserted, $rowsFailed failed in $([math]::Round($duration, 2)) seconds" "WARNING"
                $status = "PARTIAL"
            } else {
                Write-Host "Import completed successfully: $rowsInserted rows inserted in $([math]::Round($duration, 2)) seconds" -ForegroundColor Green
                Write-Log "Import completed: $rowsInserted inserted in $([math]::Round($duration, 2)) seconds" "SUCCESS"
                $status = "SUCCESS"
            }
            
            Write-LogEntry -Connection $Connection -FileName $fileName -TableName $TableName -Status $status `
                -RowsProcessed $rowsProcessed -RowsInserted $rowsInserted -RowsFailed $rowsFailed `
                -BatchSize 0 -DurationSeconds $duration `
                -CsvColumnCount $csvColumns.Count -TableColumnCount $tableColumns.Count `
                -MissingColumns ($missingColumns -join ', ')
            
            $fileProcessedSuccessfully = ($status -eq "SUCCESS")
            if ($fileProcessedSuccessfully) {
                Move-CsvFile -CsvFile $CsvFile -DestinationFolder $ProcessedFolder -Status "Processed" | Out-Null
            } else {
                Write-Host "Moving failed file back to source folder..." -ForegroundColor Yellow
                Move-CsvFile -CsvFile $CsvFile -DestinationFolder $SourceFolder -Status "Source" | Out-Null
            }
                
        } catch {
            $duration = ((Get-Date) - $script:fileStartTime).TotalSeconds
            $errorMsg = $_.Exception.Message
            Write-Host ""
            Write-Host "ERROR: Import failed for $fileName" -ForegroundColor Red
            Write-Host "Error message: $errorMsg" -ForegroundColor Red
            Write-Log "Import failed: $errorMsg" "ERROR"
            Write-LogEntry -Connection $Connection -FileName $fileName -TableName $TableName -Status "FAILED" `
                -RowsProcessed 0 -RowsInserted 0 -RowsFailed 0 -BatchSize 0 `
                -ErrorMessage $errorMsg -DurationSeconds $duration
            
            Write-Host "Moving failed file back to source folder..." -ForegroundColor Yellow
            Move-CsvFile -CsvFile $CsvFile -DestinationFolder $SourceFolder -Status "Source" | Out-Null
        }
        
        return $fileProcessedSuccessfully
    }
    
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  CSV TO SQL IMPORT PROCESS STARTING" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Log "=== CSV Import Process Started ===" "SUCCESS"
    Write-Log "Server: $ServerInstance, Database: $Database"
    Write-Log "CSV Directory: $CsvDirectory"
    Write-Log "Truncate Before Import: $TruncateBeforeImport"
    Write-LogToFile "=== CSV Import Process Started ===" "INFO"
    Write-LogToFile "Server: $ServerInstance, Database: $Database" "INFO"
    
    Write-Host ""
    Write-Host "=== PRE-CHECK: Testing Database Connection ===" -ForegroundColor Yellow
    Write-Host ""
    
    $connectionString = "Server=$ServerInstance;Database=$Database;"
    
    if ($IntegratedSecurity) {
        $connectionString += "Integrated Security=True;"
        Write-Log "Using Integrated Security authentication" "INFO"
    } elseif ($Username -and $Password) {
        $connectionString += "User Id=$Username;Password=$Password;"
        Write-Log "Using SQL Server authentication (User: $Username)" "INFO"
    } else {
        $errorMsg = "Either IntegratedSecurity must be specified or both Username and Password must be provided"
        Write-Log $errorMsg "ERROR"
        Write-LogToFile $errorMsg "ERROR"
        throw $errorMsg
    }
    
    if (-not (Test-DatabaseConnection -ConnectionString $connectionString -ServerInstance $ServerInstance -Database $Database)) {
        Write-Host ""
        $errorMsg = "Pre-check failed: Cannot connect to database. Script execution aborted."
        Write-Log $errorMsg "ERROR"
        Write-LogToFile $errorMsg "ERROR"
        Write-Log "Check log file for details: $logFilePath" "ERROR"
        Write-Host ""
        exit 1
    }
    
    Write-Host ""
    Write-Host "=== PRE-CHECK PASSED: Proceeding with import ===" -ForegroundColor Green
    Write-Host ""
    
    $sqlConnection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    
    try {
        Write-Host "Opening database connection..." -ForegroundColor Yellow
        $sqlConnection.Open()
        Write-Log "Connected to database" "SUCCESS"
        Write-LogToFile "Connected to database" "SUCCESS"
        Write-Host "Database connection established successfully!" -ForegroundColor Green
        Write-Host ""
        
        Write-Host "Initializing log table..." -ForegroundColor Yellow
        New-LogTable -Connection $sqlConnection
        Write-Host "Log table ready." -ForegroundColor Green
        Write-Host ""
        
        if (-not [System.IO.Path]::IsPathRooted($CsvDirectory)) {
            $CsvDirectory = Join-Path (Get-Location).Path $CsvDirectory
        }
        
        $CsvDirectory = $CsvDirectory.TrimEnd('\', '/')
        $CsvDirectory = [System.IO.Path]::GetFullPath($CsvDirectory)
        
        Write-Host "Initializing processing folders..." -ForegroundColor Yellow
        $folders = Initialize-ProcessingFolders -BaseDirectory $CsvDirectory
        Write-Host "Processing folders ready." -ForegroundColor Green
        Write-Host ""
        
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host "  SCANNING FOR CSV FILES" -ForegroundColor Cyan
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host ""
        Write-Log "Looking for CSV files in: $CsvDirectory" "INFO"
        $csvFiles = Get-ChildItem $CsvDirectory -Filter "*.csv" -File | Sort-Object Name
        
        if ($csvFiles.Count -eq 0) {
            Write-Host "WARNING: No CSV files found in directory: $CsvDirectory" -ForegroundColor Yellow
            Write-Log "Found 0 CSV files to process" "WARNING"
            Write-Host ""
            Write-Host "Script completed with no files to process." -ForegroundColor Yellow
            return
        }
        
        Write-Host "Found $($csvFiles.Count) CSV file(s) to process:" -ForegroundColor Green
        foreach ($f in $csvFiles) {
            Write-Host "  - $($f.Name)" -ForegroundColor White
        }
        Write-Host ""
        Write-Log "Found $($csvFiles.Count) CSV files to process"
        
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host "  PROCESSING FILES" -ForegroundColor Cyan
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host ""
        
        $fileIndex = 0
        foreach ($file in $csvFiles) {
            $fileIndex++
            Write-Host "--- File $fileIndex of $($csvFiles.Count): $($file.Name) ---" -ForegroundColor Cyan
            Write-Host ""
            $tableName = ($file.Name -replace 'SICTL_|SIDTA_', '') -replace '_\d{8}.*\.csv$', '' -replace '\.csv$', ''
            
            Write-Host "Target table: $tableName" -ForegroundColor Yellow
            
            if ([string]::IsNullOrWhiteSpace($tableName)) {
                Write-Host "ERROR: Could not determine table name from file: $($file.Name)" -ForegroundColor Red
                Write-Log "Could not determine table name from file: $($file.Name)" "WARNING"
                Write-Host "Moving file back to source folder..." -ForegroundColor Yellow
                Move-CsvFile -CsvFile $file -DestinationFolder $CsvDirectory -Status "Source" | Out-Null
                Write-Host ""
                continue
            }
            
            if ($TruncateBeforeImport) {
                Import-CsvFile -Connection $sqlConnection -CsvFile $file -TableName $tableName -ProcessedFolder $folders.Processed -ErrorFolder $folders.Error -SourceFolder $CsvDirectory -TruncateBeforeImport -TempFolder $TempFolder -TruncateTableForFiles $TruncateTableForFiles
            } else {
                Import-CsvFile -Connection $sqlConnection -CsvFile $file -TableName $tableName -ProcessedFolder $folders.Processed -ErrorFolder $folders.Error -SourceFolder $CsvDirectory -TempFolder $TempFolder -TruncateTableForFiles $TruncateTableForFiles
            }
            Write-Host ""
            Write-Host "--- Completed file $fileIndex of $($csvFiles.Count) ---" -ForegroundColor Cyan
            Write-Host ""
        }
        
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host "  IMPORT PROCESS COMPLETED" -ForegroundColor Cyan
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host ""
        $totalDuration = ((Get-Date) - $startTime).TotalSeconds
        Write-Log "=== Import Process Completed in $([math]::Round($totalDuration, 2)) seconds ===" "SUCCESS"
        Write-Host "Total processing time: $([math]::Round($totalDuration, 2)) seconds" -ForegroundColor Green
        Write-Host "Processed $($csvFiles.Count) file(s)" -ForegroundColor Green
        Write-Host ""
        
    } catch {
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Red
        Write-Host "  FATAL ERROR OCCURRED" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
        Write-Host ""
        $errorMsg = "Fatal error: $_"
        Write-Host $errorMsg -ForegroundColor Red
        Write-Log $errorMsg "ERROR"
        Write-LogToFile $errorMsg "ERROR"
        if ($_.Exception.InnerException) {
            $innerError = "Inner exception: $($_.Exception.InnerException.Message)"
            Write-Host $innerError -ForegroundColor Red
            Write-Log $innerError "ERROR"
            Write-LogToFile $innerError "ERROR"
        }
        Write-Host ""
        Write-Host "Check log file for details: $logFilePath" -ForegroundColor Yellow
        Write-Host ""
        throw
    } finally {
        if ($sqlConnection.State -eq "Open") {
            Write-Host "Closing database connection..." -ForegroundColor Yellow
            $sqlConnection.Close()
            Write-Host "Database connection closed." -ForegroundColor Green
        }
    }
}

Write-Host ""
Write-Host "Starting CSV Import Script..." -ForegroundColor Cyan
Write-Host "Press Ctrl+C to cancel at any time" -ForegroundColor Gray
Write-Host ""
Import-CsvToSql -CsvDirectory $CsvDirectory -ServerInstance $ServerInstance -Database $Database -Username $Username -Password $Password -IntegratedSecurity:$IntegratedSecurity -TruncateBeforeImport $TruncateBeforeImport -TempFolder $TempFolder -TruncateTableForFiles $TruncateTableForFiles

