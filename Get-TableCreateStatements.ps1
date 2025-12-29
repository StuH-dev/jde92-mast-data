function Get-TableCreateStatements {
    param(
        [string]$ServerInstance = 'jdesql01',
        [string]$Database = 'JDE_PRODUCTION',
        [string]$TableList = "F03012",
        [string]$Username = 'sa',
        [string]$Password = 'Pcare2009',
        [switch]$IntegratedSecurity
    )
    
    $ErrorActionPreference = "Stop"
    
    try {
        $tables = $TableList -split ',' | ForEach-Object { $_.Trim() }
        
        if ($tables.Count -eq 0) {
            throw "No tables specified in TableList"
        }
        
        $connectionString = "Server=$ServerInstance;Database=$Database;"
        
        if ($IntegratedSecurity) {
            $connectionString += "Integrated Security=True;"
        } elseif ($Username -and $Password) {
            $connectionString += "User Id=$Username;Password=$Password;"
        } else {
            throw "Either IntegratedSecurity must be specified or both Username and Password must be provided"
        }
        
        $sqlConnection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        $sqlConnection.Open()
        
        $createStatements = @()
        
        $tableValues = ($tables | ForEach-Object { "('$_')" }) -join ",`n            "
        
        $query = @"
DECLARE @TableNames TABLE (TableName NVARCHAR(128))
INSERT INTO @TableNames VALUES 
            $tableValues

SELECT 
    QUOTENAME(s.name) + '.' + QUOTENAME(t.name) AS FullTableName,
    s.name AS SchemaName,
    t.name AS TableName,
    STUFF((
        SELECT 
            ',' + CHAR(10) + '    ' + QUOTENAME(c.name) + ' ' +
            CASE 
                WHEN ty.name IN ('varchar', 'nvarchar', 'char', 'nchar', 'binary', 'varbinary') THEN
                    ty.name + CASE 
                        WHEN c.max_length = -1 THEN '(MAX)'
                        WHEN ty.name IN ('nvarchar', 'nchar') THEN '(' + CAST(c.max_length/2 AS VARCHAR) + ')'
                        ELSE '(' + CAST(c.max_length AS VARCHAR) + ')'
                    END
                WHEN ty.name IN ('decimal', 'numeric') THEN
                    ty.name + '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
                WHEN ty.name IN ('float', 'real') THEN
                    ty.name + CASE WHEN c.precision <> 53 THEN '(' + CAST(c.precision AS VARCHAR) + ')' ELSE '' END
                WHEN ty.name IN ('datetime2', 'datetimeoffset', 'time') THEN
                    ty.name + CASE WHEN c.scale > 0 THEN '(' + CAST(c.scale AS VARCHAR) + ')' ELSE '' END
                ELSE ty.name
            END +
            CASE WHEN c.is_nullable = 0 THEN ' NOT NULL' ELSE ' NULL' END +
            CASE 
                WHEN dc.name IS NOT NULL THEN ' DEFAULT ' + dc.definition
                ELSE ''
            END
        FROM sys.tables tbl
        INNER JOIN sys.schemas s2 ON tbl.schema_id = s2.schema_id
        INNER JOIN sys.columns c ON tbl.object_id = c.object_id
        INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
        WHERE tbl.name = t.name AND s2.name = s.name
        ORDER BY c.column_id
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS ColumnDefinitions,
    (
        SELECT 
            ',' + CHAR(10) + '    CONSTRAINT ' + QUOTENAME(kc.name) + ' PRIMARY KEY (' +
            STUFF((
                SELECT ', ' + QUOTENAME(c.name)
                FROM sys.index_columns ic
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
                ORDER BY ic.key_ordinal
                FOR XML PATH(''), TYPE
            ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')'
        FROM sys.key_constraints kc
        WHERE kc.parent_object_id = t.object_id AND kc.type = 'PK'
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)') AS PrimaryKeyConstraint
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN @TableNames tn ON t.name = tn.TableName
WHERE t.type = 'U'
ORDER BY s.name, t.name
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $sqlConnection)
        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($command)
        $dataset = New-Object System.Data.DataSet
        $adapter.Fill($dataset) | Out-Null
        
        if ($dataset.Tables[0].Rows.Count -eq 0) {
            Write-Warning "No tables found matching the provided table names"
            $sqlConnection.Close()
            return @()
        }
        
        foreach ($row in $dataset.Tables[0].Rows) {
            $fullTableName = $row["FullTableName"]
            $columnDefinitions = $row["ColumnDefinitions"]
            $primaryKeyConstraint = $row["PrimaryKeyConstraint"]
            
            if ([string]::IsNullOrWhiteSpace($columnDefinitions)) {
                Write-Warning "Could not generate CREATE statement for table '$fullTableName'"
                continue
            }
            
            $createStatement = "CREATE TABLE $fullTableName (`n"
            $createStatement += $columnDefinitions
            
            if (-not [string]::IsNullOrWhiteSpace($primaryKeyConstraint)) {
                $createStatement += $primaryKeyConstraint
            }
            
            $createStatement += "`n);"
            
            $createStatements += $createStatement
        }
        
        $sqlConnection.Close()
        
        return $createStatements
    }
    catch {
        if ($sqlConnection -and $sqlConnection.State -eq "Open") {
            $sqlConnection.Close()
        }
        Write-Error "Error generating CREATE statements: $_"
        throw
    }
}

$outputFile = 'CreateStatements.sql'
$statements = Get-TableCreateStatements
if ($statements.Count -gt 0) {
    $statements | Out-File -FilePath $outputFile -Encoding UTF8
    Write-Host "CREATE statements written to: $outputFile" -ForegroundColor Green
    Write-Host "Generated $($statements.Count) table definition(s)" -ForegroundColor Green
} else {
    Write-Warning "No CREATE statements generated"
}

