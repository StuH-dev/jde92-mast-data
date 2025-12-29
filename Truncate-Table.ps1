$connString = "Server=localhost\SQLExpress;Database=sales-dashboard;User Id=sa;Password=Pcare2009;"
$conn = New-Object System.Data.SqlClient.SqlConnection($connString)

try {
    $conn.Open()
    $cmd = New-Object System.Data.SqlClient.SqlCommand("TRUNCATE TABLE [DBO].[F40942]", $conn)
    $cmd.ExecuteNonQuery() | Out-Null
    Write-Host "Table F40942 truncated successfully" -ForegroundColor Green
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
} finally {
    $conn.Close()
}

