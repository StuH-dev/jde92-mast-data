function Analyze-CsvFiles {
    param(
        [string]$CsvDirectory = "DATA_FILES_TO_IMPORT",
        [string]$CreateStatementsFile = "CreateStatements.sql"
    )
    
    $ErrorActionPreference = "Stop"
    
    Write-Host "=== CSV File Analysis ===" -ForegroundColor Cyan
    Write-Host ""
    
    $lines = Get-Content $CreateStatementsFile
    $tableDefinitions = @{}
    $currentTable = $null
    $currentColumns = @()
    
    foreach ($line in $lines) {
        $line = $line.Trim()
        
        if ($line -match 'CREATE TABLE\s+\[DBO\]\.\[(\w+)\]') {
            if ($currentTable -and $currentColumns.Count -gt 0) {
                $tableDefinitions[$currentTable] = $currentColumns
            }
            $currentTable = $matches[1]
            $currentColumns = @()
        }
        elseif ($currentTable -and $line -match '\[(\w+)\]\s+(\w+(?:\([^)]+\))?)\s+(NOT NULL|NULL)') {
            $currentColumns += [PSCustomObject]@{
                Name = $matches[1]
                Type = $matches[2]
                Nullable = $matches[3] -eq 'NULL'
            }
        }
        elseif ($line -match 'CONSTRAINT.*PRIMARY KEY') {
            if ($currentTable -and $currentColumns.Count -gt 0) {
                $tableDefinitions[$currentTable] = $currentColumns
            }
            $currentTable = $null
            $currentColumns = @()
        }
    }
    
    if ($currentTable -and $currentColumns.Count -gt 0) {
        $tableDefinitions[$currentTable] = $currentColumns
    }
    
    Write-Host "Found $($tableDefinitions.Count) table definitions" -ForegroundColor Green
    Write-Host ""
    
    $csvFiles = Get-ChildItem $CsvDirectory -Filter "*.csv"
    $issues = @()
    $analysis = @()
    
    foreach ($file in $csvFiles) {
        $tableName = ($file.Name -replace 'SICTL_|SIDTA_', '') -replace '_\d{8}_\d{6}.*\.csv$', ''
        
        $fileInfo = [PSCustomObject]@{
            FileName = $file.Name
            TableName = $tableName
            FileSize = $file.Length
            HasData = $false
            RowCount = 0
            ColumnCount = 0
            CsvColumns = @()
            TableColumns = @()
            MissingColumns = @()
            ExtraColumns = @()
            Issues = @()
        }
        
        if (-not $tableDefinitions.ContainsKey($tableName)) {
            $fileInfo.Issues += "Table '$tableName' not found in CreateStatements.sql"
            $issues += $fileInfo
            $analysis += $fileInfo
            continue
        }
        
        $fileInfo.TableColumns = $tableDefinitions[$tableName].Name
        
        try {
            $content = Get-Content $file.FullName -Encoding UTF8
            
            if ($content.Count -eq 0) {
                $fileInfo.Issues += "File is completely empty"
                $issues += $fileInfo
                $analysis += $fileInfo
                continue
            }
            
            $headerLine = $content[0]
            $fileInfo.CsvColumns = ($headerLine -replace '"', '') -split ',' | ForEach-Object { $_.Trim() }
            $fileInfo.ColumnCount = $fileInfo.CsvColumns.Count
            
            if ($content.Count -gt 1) {
                $fileInfo.HasData = $true
                $fileInfo.RowCount = $content.Count - 1
            } else {
                $fileInfo.Issues += "File contains only header row, no data"
            }
            
            $fileInfo.MissingColumns = $fileInfo.TableColumns | Where-Object { $fileInfo.CsvColumns -notcontains $_ }
            $fileInfo.ExtraColumns = $fileInfo.CsvColumns | Where-Object { $fileInfo.TableColumns -notcontains $_ }
            
            if ($fileInfo.MissingColumns.Count -gt 0) {
                $fileInfo.Issues += "Missing columns in CSV: $($fileInfo.MissingColumns -join ', ')"
            }
            
            if ($fileInfo.ExtraColumns.Count -gt 0) {
                $fileInfo.Issues += "Extra columns in CSV (not in table): $($fileInfo.ExtraColumns -join ', ')"
            }
            
            if ($fileInfo.Issues.Count -gt 0) {
                $issues += $fileInfo
            }
            
        } catch {
            $fileInfo.Issues += "Error reading file: $_"
            $issues += $fileInfo
        }
        
        $analysis += $fileInfo
    }
    
    Write-Host "=== Summary ===" -ForegroundColor Cyan
    Write-Host "Total CSV files: $($csvFiles.Count)"
    Write-Host "Files with data: $(($analysis | Where-Object { $_.HasData }).Count)"
    Write-Host "Empty files: $(($analysis | Where-Object { -not $_.HasData }).Count)"
    Write-Host "Files with issues: $($issues.Count)"
    Write-Host ""
    
    if ($issues.Count -gt 0) {
        Write-Host "=== Issues Found ===" -ForegroundColor Yellow
        foreach ($issue in $issues) {
            Write-Host ""
            Write-Host "File: $($issue.FileName)" -ForegroundColor Yellow
            Write-Host "  Table: $($issue.TableName)"
            Write-Host "  Row Count: $($issue.RowCount)"
            Write-Host "  CSV Columns: $($issue.ColumnCount)"
            Write-Host "  Table Columns: $($issue.TableColumns.Count)"
            foreach ($issueMsg in $issue.Issues) {
                Write-Host "  ISSUE: $issueMsg" -ForegroundColor Red
            }
        }
        Write-Host ""
    }
    
    Write-Host "=== Detailed Analysis ===" -ForegroundColor Cyan
    foreach ($item in $analysis) {
        Write-Host ""
        Write-Host "File: $($item.FileName)" -ForegroundColor White
        Write-Host "  Table: $($item.TableName)"
        Write-Host "  Size: $($item.FileSize) bytes"
        Write-Host "  Has Data: $($item.HasData)"
        Write-Host "  Row Count: $($item.RowCount)"
        Write-Host "  CSV Columns ($($item.ColumnCount)): $($item.CsvColumns -join ', ')"
        Write-Host "  Table Columns ($($item.TableColumns.Count)): $($item.TableColumns -join ', ')"
        if ($item.MissingColumns.Count -gt 0) {
            Write-Host "  Missing: $($item.MissingColumns -join ', ')" -ForegroundColor Yellow
        }
        if ($item.ExtraColumns.Count -gt 0) {
            Write-Host "  Extra: $($item.ExtraColumns -join ', ')" -ForegroundColor Yellow
        }
    }
    
    return @{
        Analysis = $analysis
        Issues = $issues
        TableDefinitions = $tableDefinitions
    }
}

$result = Analyze-CsvFiles

