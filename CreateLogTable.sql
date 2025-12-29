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
    CREATE INDEX [IX_CSV_IMPORT_LOG_Status] ON [DBO].[CSV_IMPORT_LOG] ([Status]);
END
GO

