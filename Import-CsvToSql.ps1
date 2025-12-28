param(
    [string]$CsvDirectory = "DATA_FILES_TO_IMPORT",
    [string]$ServerInstance = 'localhost\SQLExpress',
    [string]$Database = 'sales-dashboard',
    [string]$Username = 'sa',
    [string]$Password = 'Pcare2009',
    [switch]$IntegratedSecurity,
    [switch]$TruncateBeforeImport,
    [string]$TempFolder = "C:\Temp\CSV_Import"
)

function Import-CsvToSql {
    param(
        [string]$CsvDirectory = "DATA_FILES_TO_IMPORT",
        [string]$ServerInstance = 'localhost\SQLExpress',
        [string]$Database = 'sales-dashboard',
        [string]$Username = 'sa',
        [string]$Password = 'Pcare2009',
        [switch]$IntegratedSecurity,
        [switch]$TruncateBeforeImport,
        [string]$TempFolder = "C:\Temp\CSV_Import"
    )
    
    $ErrorActionPreference = "Stop"
    
    $logTableName = "[DBO].[CSV_IMPORT_LOG]"
    $startTime = Get-Date
    
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
                $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
                $nameWithoutExt = [System.IO.Path]::GetFileNameWithoutExtension($CsvFile.Name)
                $extension = $CsvFile.Extension
                $destinationPath = Join-Path $DestinationFolder "$nameWithoutExt`_$timestamp$extension"
            }
            
            Move-Item -Path $CsvFile.FullName -Destination $destinationPath -Force
            Write-Log "Moved file to $Status folder: $($CsvFile.Name)" "SUCCESS"
            return $true
        } catch {
            Write-Log "Failed to move file $($CsvFile.Name) to $Status folder: $_" "ERROR"
            return $false
        }
    }
    
    function Ensure-LogTable {
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
        $tempFilePath = $null
        
        try {
            if (-not (Test-Path $TempFolder)) {
                New-Item -ItemType Directory -Path $TempFolder -Force | Out-Null
                Write-Log "Created temp folder: $TempFolder" "INFO"
            }
            
            $tempFileName = "$($CsvFile.BaseName)_$([System.Guid]::NewGuid().ToString().Replace('-', ''))$($CsvFile.Extension)"
            $tempFilePath = Join-Path $TempFolder $tempFileName
            
            Write-Log "Preparing CSV file for BULK INSERT (converting to unquoted format): $tempFilePath" "INFO"
            
            $csvData = Import-Csv -Path $CsvFile.FullName -Encoding UTF8
            $csvLines = @()
            
            foreach ($row in $csvData) {
                $values = @()
                foreach ($colName in $MatchingColumns) {
                    $val = $row.$colName
                    if ($null -eq $val) {
                        $val = ""
                    }
                    $values += $val
                }
                $csvLines += ($values -join ',')
            }
            
            $headerLine = $MatchingColumns -join ','
            $allLines = @($headerLine) + $csvLines
            $allLines | Set-Content -Path $tempFilePath -Encoding UTF8
            
            $filePath = $tempFilePath.Replace('\', '\\')
            
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
            
            Write-Log "Loading data into staging table using BULK INSERT" "INFO"
            
            $bulkInsertQuery = @"
BULK INSERT [$tempTableName]
FROM '$filePath'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    CODEPAGE = '65001',
    TABLOCK
)
"@
            
            $command = New-Object System.Data.SqlClient.SqlCommand($bulkInsertQuery, $Connection)
            $command.ExecuteNonQuery() | Out-Null
            
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
                $dropQuery = "IF OBJECT_ID('tempdb..[$tempTableName]') IS NOT NULL DROP TABLE [$tempTableName]"
                $command = New-Object System.Data.SqlClient.SqlCommand($dropQuery, $Connection)
                $command.ExecuteNonQuery() | Out-Null
            } catch {
                Write-Log "Warning: Could not drop temporary table $tempTableName" "WARNING"
            }
            throw
        } finally {
            if ($null -ne $tempFilePath -and (Test-Path $tempFilePath)) {
                try {
                    Remove-Item -Path $tempFilePath -Force -ErrorAction SilentlyContinue
                    Write-Log "Cleaned up temporary file: $tempFilePath" "INFO"
                } catch {
                    Write-Log "Warning: Could not delete temporary file $tempFilePath" "WARNING"
                }
            }
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
            [switch]$TruncateBeforeImport,
            [string]$TempFolder
        )
        
        $script:fileStartTime = Get-Date
        $fileName = $CsvFile.Name
        $fileProcessedSuccessfully = $false
        
        Write-Log "Processing file: $fileName -> Table: $TableName"
        
        try {
            Write-Log "Step 1: Checking file path" "INFO"
            if (-not (Test-Path $CsvFile.FullName)) {
                throw "File not found: $($CsvFile.FullName)"
            }
            
            Write-Log "Step 2: Checking file size" "INFO"
            if ($CsvFile.Length -eq 0) {
                Write-Log "File is empty, treating as success" "WARNING"
                Write-LogEntry -Connection $Connection -FileName $fileName -TableName $TableName -Status "SUCCESS" -DurationSeconds 0 -CsvColumnCount 0 -TableColumnCount 0 -ErrorMessage "File is empty"
                $fileProcessedSuccessfully = $true
                Move-CsvFile -CsvFile $CsvFile -DestinationFolder $ProcessedFolder -Status "Processed" | Out-Null
                return $fileProcessedSuccessfully
            }
            
            Write-Log "Step 3: Importing CSV" "INFO"
            try {
                $csvData = Import-Csv -Path $CsvFile.FullName -Encoding UTF8 -ErrorAction Stop
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
            
            Write-Log "Step 5: Checking array count (Count: $($csvDataArray.Count))" "INFO"
            if ($csvDataArray.Count -eq 0) {
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
            
            Write-Log "Step 7: Getting CSV columns" "INFO"
            try {
                $csvColumns = ($csvDataArray[0] | Get-Member -MemberType NoteProperty).Name
            } catch {
                throw "Failed to get column names from CSV file '$fileName': $_"
            }
            
            if ($null -eq $csvColumns -or $csvColumns.Count -eq 0) {
                throw "Could not determine CSV column names from file '$fileName'"
            }
            
            Write-Log "Step 8: Getting table columns" "INFO"
            $tableColumns = Get-TableColumns -Connection $Connection -TableName $TableName
            
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
            
            Write-Log "CSV columns: $($csvColumns.Count), Table columns: $($tableColumnInfo.Count), Matching: $($matchingColumns.Count)"
            
            if ($missingRequiredColumns.Count -gt 0) {
                $csvColList = $csvColumns -join ', '
                $missingReqColList = $missingRequiredColumns -join ', '
                throw "Missing required (non-nullable) columns in CSV file '$fileName' for table '$TableName'. CSV contains columns: [$csvColList]. Missing required columns: [$missingReqColList]"
            }
            
            if ($missingColumns.Count -gt 0) {
                Write-Log "Missing optional columns (will be set to NULL): $($missingColumns -join ', ')" "WARNING"
            }
            
            Write-Log "Step 11: Preparing bulk insert" "INFO"
            
            if ($TruncateBeforeImport) {
                Write-Log "Truncating table before import..." "WARNING"
                $truncateQuery = "TRUNCATE TABLE [DBO].[$TableName]"
                $command = New-Object System.Data.SqlClient.SqlCommand($truncateQuery, $Connection)
                $command.ExecuteNonQuery() | Out-Null
                Write-Log "Table truncated successfully" "SUCCESS"
            }
            
            $rowsProcessed = $csvDataArray.Count
            $rowsInserted = 0
            $rowsFailed = 0
            
            try {
                Write-Log "Step 12: Executing SQL Server BULK INSERT ($rowsProcessed rows)" "INFO"
                $rowsInserted = Invoke-SqlBulkInsert -Connection $Connection -CsvFile $CsvFile -TableName $TableName -MatchingColumns $matchingColumns -TableColumnInfo $tableColumnInfo -TempFolder $TempFolder
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
                Move-CsvFile -CsvFile $CsvFile -DestinationFolder $ErrorFolder -Status "Error" | Out-Null
            }
                
        } catch {
            $duration = ((Get-Date) - $script:fileStartTime).TotalSeconds
            $errorMsg = $_.Exception.Message
            Write-Log "Import failed: $errorMsg" "ERROR"
            Write-LogEntry -Connection $Connection -FileName $fileName -TableName $TableName -Status "FAILED" `
                -RowsProcessed 0 -RowsInserted 0 -RowsFailed 0 -BatchSize 0 `
                -ErrorMessage $errorMsg -DurationSeconds $duration
            
            Move-CsvFile -CsvFile $CsvFile -DestinationFolder $ErrorFolder -Status "Error" | Out-Null
        }
        
        return $fileProcessedSuccessfully
    }
    
    Write-Log "=== CSV Import Process Started ===" "SUCCESS"
    Write-Log "Server: $ServerInstance, Database: $Database"
    
    $connectionString = "Server=$ServerInstance;Database=$Database;"
    
    if ($IntegratedSecurity) {
        $connectionString += "Integrated Security=True;"
    } elseif ($Username -and $Password) {
        $connectionString += "User Id=$Username;Password=$Password;"
    } else {
        throw "Either IntegratedSecurity must be specified or both Username and Password must be provided"
    }
    
    $sqlConnection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    
    try {
        $sqlConnection.Open()
        Write-Log "Connected to database" "SUCCESS"
        
        Ensure-LogTable -Connection $sqlConnection
        
        if (-not [System.IO.Path]::IsPathRooted($CsvDirectory)) {
            $CsvDirectory = Join-Path (Get-Location).Path $CsvDirectory
        }
        
        $CsvDirectory = $CsvDirectory.TrimEnd('\', '/')
        $CsvDirectory = [System.IO.Path]::GetFullPath($CsvDirectory)
        
        $folders = Initialize-ProcessingFolders -BaseDirectory $CsvDirectory
        
        Write-Log "Looking for CSV files in: $CsvDirectory" "INFO"
        $csvFiles = Get-ChildItem $CsvDirectory -Filter "*.csv" -File | Sort-Object Name
        
        Write-Log "Found $($csvFiles.Count) CSV files to process"
        
        foreach ($file in $csvFiles) {
            $tableName = ($file.Name -replace 'SICTL_|SIDTA_', '') -replace '_\d{8}_\d{6}.*\.csv$', ''
            
            if ([string]::IsNullOrWhiteSpace($tableName)) {
                Write-Log "Could not determine table name from file: $($file.Name)" "WARNING"
                Move-CsvFile -CsvFile $file -DestinationFolder $folders.Error -Status "Error" | Out-Null
                continue
            }
            
            if ($TruncateBeforeImport.IsPresent) {
                Import-CsvFile -Connection $sqlConnection -CsvFile $file -TableName $tableName -ProcessedFolder $folders.Processed -ErrorFolder $folders.Error -TruncateBeforeImport -TempFolder $TempFolder
            } else {
                Import-CsvFile -Connection $sqlConnection -CsvFile $file -TableName $tableName -ProcessedFolder $folders.Processed -ErrorFolder $folders.Error -TempFolder $TempFolder
            }
            Write-Host ""
        }
        
        $totalDuration = ((Get-Date) - $startTime).TotalSeconds
        Write-Log "=== Import Process Completed in $([math]::Round($totalDuration, 2)) seconds ===" "SUCCESS"
        
    } catch {
        Write-Log "Fatal error: $_" "ERROR"
        throw
    } finally {
        if ($sqlConnection.State -eq "Open") {
            $sqlConnection.Close()
        }
    }
}

if ($MyInvocation.InvocationName -ne '.') {
    Import-CsvToSql -CsvDirectory $CsvDirectory -ServerInstance $ServerInstance -Database $Database -Username $Username -Password $Password -IntegratedSecurity:$IntegratedSecurity -TruncateBeforeImport:$TruncateBeforeImport -TempFolder $TempFolder
}

