-- Load reference/transaction_mapping_seed.csv into this table.
-- Example after uploading the CSV to a Unity Catalog Volume:
-- COPY INTO ng_banking_lakehouse.transaction_curated.transaction_mapping
-- FROM '/Volumes/ng_banking_lakehouse/transaction_raw/transaction_files/reference/transaction_mapping_seed.csv'
-- FILEFORMAT = CSV FORMAT_OPTIONS ('header'='true') COPY_OPTIONS ('mergeSchema'='true');

-- Confirmed mappings from the supplied design:
INSERT INTO ng_banking_lakehouse.transaction_curated.transaction_mapping VALUES
('F23',NULL,NULL,100,'FUNDS_TRANSFER','TRANSFER','Funds Transfer','TRANSFER','Funds Transfer',FALSE,'Y'),
('F24',NULL,NULL,100,'FUNDS_TRANSFER','TRANSFER','Funds Transfer','TRANSFER','Funds Transfer',FALSE,'Y'),
('T10',NULL,NULL,100,'ACCOUNT_SERVICING','ACCOUNT_TRANSACTION','Account Transaction','ACCOUNT_TRANSACTION','Account Transaction',FALSE,'Y');

-- Module mappings are intentionally configurable. Add only modules that
-- are actually present in your source after profiling.
INSERT INTO ng_banking_lakehouse.transaction_curated.transaction_mapping VALUES
(NULL,'DE',NULL,10,'DEPOSIT','DEPOSIT','Deposit / Account Deposit','DEPOSIT','Deposit / Account Deposit',FALSE,'Y'),
(NULL,'FT',NULL,10,'FUNDS_TRANSFER','TRANSFER','Funds Transfer','TRANSFER','Funds Transfer',FALSE,'Y'),
(NULL,'CL',NULL,10,'LOAN','LENDING','Loan / Lending','LOAN','Loan / Lending',FALSE,'Y'),
(NULL,'LD',NULL,10,'LOAN','LENDING','Loan / Lending','LOAN','Loan / Lending',FALSE,'Y'),
(NULL,'LC',NULL,10,'LETTER_OF_CREDIT','TRADE_FINANCE','Letter of Credit','TRADE_FINANCE','Letter of Credit',FALSE,'Y'),
(NULL,'BC',NULL,10,'BILLS','TRADE_FINANCE','Bills / Collections','TRADE_FINANCE','Bills / Collections',FALSE,'Y'),
(NULL,'FX',NULL,10,'FOREIGN_EXCHANGE','TREASURY','Foreign Exchange','FOREIGN_EXCHANGE','Foreign Exchange',FALSE,'Y'),
(NULL,'MM',NULL,10,'MONEY_MARKET','TREASURY','Money Market','MONEY_MARKET','Money Market',FALSE,'Y'),
(NULL,'IC',NULL,10,'INTEREST','ACCOUNT_SERVICING','Interest / Charges','CHARGES','Interest / Charges',FALSE,'Y'),
(NULL,'AC',NULL,10,'ACCOUNTING','ACCOUNTING','General Accounting','ACCOUNTING','General Accounting',FALSE,'Y'),
(NULL,'ST',NULL,10,'SECURITIES','INVESTMENTS','Securities','INVESTMENT','Securities Transaction',FALSE,'Y'),
(NULL,'SI',NULL,10,'STANDING_INSTRUCTION','PAYMENTS','Standing Instruction','PAYMENT','Standing Instruction',FALSE,'Y'),
(NULL,'PM',NULL,10,'PAYMENTS','PAYMENTS','Payment Processing','PAYMENT','Payment Processing',FALSE,'Y'),
(NULL,'RM',NULL,10,'REMITTANCE','PAYMENTS','Remittance','PAYMENT','Remittance',FALSE,'Y'),
(NULL,'SW',NULL,10,'SWIFT','PAYMENTS','SWIFT Payment','TRANSFER','SWIFT Payment',FALSE,'Y'),
(NULL,'CH',NULL,10,'CHEQUE','PAYMENTS','Cheque Processing','PAYMENT','Cheque Processing',FALSE,'Y');

-- IMPORTANT: do not classify every negative amount as reversal.
-- Set IS_REVERSAL_CODE=TRUE only for verified reversal transaction codes.
-- 'REV' below is the one verified system-reversal code in this environment; it is the
-- ONLY row with IS_REVERSAL_CODE=TRUE, which is what lets notebook 03's
-- IS_REVERSAL_CANDIDATE logic (negative amount AND a verified reversal mapping) ever
-- evaluate to true, and what the 04b quality gate's
-- REVERSAL_CANDIDATE_WITHOUT_VERIFIED_CODE check protects against regressing.
INSERT INTO ng_banking_lakehouse.transaction_curated.transaction_mapping VALUES
('REV',NULL,NULL,100,'REVERSAL','REVERSAL','System Reversal','REVERSAL','System Reversal',TRUE,'Y');
