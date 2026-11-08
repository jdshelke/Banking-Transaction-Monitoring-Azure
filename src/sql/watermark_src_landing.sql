CREATE TABLE [dbo].[source_last_send] (
    [source] VARCHAR(255) NOT NULL,
    [updatedate] DATETIME2(3) NOT NULL,
    [source_id] BIGINT NOT NULL CONSTRAINT DF_source_last_send_source_id DEFAULT 0,
    [target_table_name] VARCHAR(100) NULL,
    [watermark_column] VARCHAR(100) NULL
);

INSERT INTO [dbo].[source_last_send] (
    [source],
    [updatedate],
    [source_id],
    [target_table_name],
    [watermark_column]
)
VALUES
    ('accounts', '1900-01-01 00:00:00.000', 0, 'accounts', 'updated_at'),
    ('branches', '1900-01-01 00:00:00.000', 0, 'branches', 'updated_at'),
    ('card_transactions', '1900-01-01 00:00:00.000', 0, 'card_transactions', 'updated_at'),
    ('cards', '1900-01-01 00:00:00.000', 0, 'cards', 'updated_at'),
    ('customers', '1900-01-01 00:00:00.000', 0, 'customers', NULL),
    ('employees', '1900-01-01 00:00:00.000', 0, 'employees', NULL),
    ('loan_payments', '1900-01-01 00:00:00.000', 0, 'loan_payments', 'updated_at'),
    ('loans', '1900-01-01 00:00:00.000', 0, 'loans', 'updated_at'),
    ('support_tickets', '1900-01-01 00:00:00.000', 0, 'support_tickets', 'updated_at'),
    ('transactions', '1900-01-01 00:00:00.000', 0, 'transactions', 'updated_at');