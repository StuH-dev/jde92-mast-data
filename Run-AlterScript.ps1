$connString = "Server=localhost\SQLExpress;Database=sales-dashboard;User Id=sa;Password=Pcare2009;"
$conn = New-Object System.Data.SqlClient.SqlConnection($connString)

try {
    $conn.Open()
    Write-Host "Connected to database" -ForegroundColor Green
    
    $sql = "ALTER TABLE [DBO].[F40942] DROP CONSTRAINT [F40942_PK];"
    $cmd = New-Object System.Data.SqlClient.SqlCommand($sql, $conn)
    $cmd.ExecuteNonQuery() | Out-Null
    Write-Host "Dropped old primary key constraint" -ForegroundColor Green
    
    $sql = "ALTER TABLE [DBO].[F40942] ALTER COLUMN [CKCGID] float NOT NULL;"
    $cmd.CommandText = $sql
    $cmd.ExecuteNonQuery() | Out-Null
    Write-Host "Updated CKCGID to NOT NULL" -ForegroundColor Green
    
    $sql = "ALTER TABLE [DBO].[F40942] ADD CONSTRAINT [F40942_PK] PRIMARY KEY ([CKCPGP], [CKCGP1], [CKCGP2], [CKCGP3], [CKCGP4], [CKCGP5], [CKCGP6], [CKCGP7], [CKCGP8], [CKCGP9], [CKCGP10], [CKCGID]);"
    $cmd.CommandText = $sql
    $cmd.ExecuteNonQuery() | Out-Null
    Write-Host "Added new primary key with CKCGID" -ForegroundColor Green
    
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
} finally {
    $conn.Close()
}

