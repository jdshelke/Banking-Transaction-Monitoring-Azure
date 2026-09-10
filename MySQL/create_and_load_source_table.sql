-- create database if not exists banking;
-- use banking;

-- ######################################## loans.csv ##################################
CREATE TABLE loans (
    loan_id BIGINT,
    customer_id BIGINT,
    branch_id BIGINT,
    loan_type VARCHAR(50),
    loan_amount DECIMAL(18,2),
    interest_rate DECIMAL(5,2),
    term_months INT,
    start_date DATE,
    status VARCHAR(20),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (loan_id)
);

-- ######################################## support_tickets ################################
CREATE TABLE support_tickets (
    ticket_id BIGINT,
    customer_id BIGINT,
    issue_type VARCHAR(100),
    date_opened DATE,
    date_resolved DATE,
    status VARCHAR(30),
    satisfaction_score INT,
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (ticket_id)
);

-- ###################################### transactions ##########################
CREATE TABLE transactions (
    transaction_id BIGINT,
    account_id BIGINT,
    txn_date DATE,
    txn_type VARCHAR(30),
    amount DECIMAL(18,2),
    channel VARCHAR(50),
    merchant_category VARCHAR(100),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (transaction_id)
);

################################## customers ########################
CREATE TABLE customers (
    customer_id BIGINT,
    name VARCHAR(100),
    gender VARCHAR(20),
    date_of_birth DATE,
    city VARCHAR(100),
    state VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(150),
    occupation VARCHAR(100),
    annual_income DECIMAL(18,2),
    join_date DATE,
    credit_score INT,
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (customer_id)
);

##################################### employees.csv ########################
CREATE TABLE employees (
    employee_id BIGINT,
    name VARCHAR(100),
    branch_id BIGINT,
    role VARCHAR(100),
    hire_date DATE,
    salary DECIMAL(18,2),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (employee_id)
);

################################################ loan_payments.csv #######################
CREATE TABLE loan_payments (
    payment_id BIGINT,
    loan_id BIGINT,
    payment_date DATE,
    amount_paid DECIMAL(18,2),
    principal_component DECIMAL(18,2),
    interest_component DECIMAL(18,2),
    late_payment_flag BOOLEAN,
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (payment_id)
);

################################### cards.csv #######################
CREATE TABLE cards (
    card_id BIGINT,
    customer_id BIGINT,
    account_id BIGINT,
    card_type VARCHAR(50),
    issue_date DATE,
    expiry_date DATE,
    credit_limit DECIMAL(18,2),
    status VARCHAR(30),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (card_id)
);

############################### card_transactions.csv #####################
CREATE TABLE card_transactions (
    card_txn_id BIGINT,
    card_id BIGINT,
    txn_date DATE,
    merchant_category VARCHAR(100),
    amount DECIMAL(18,2),
    is_fraud BOOLEAN,
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (card_txn_id)
);

##################################### accounts.csv ########################
CREATE TABLE accounts (
    account_id BIGINT,
    customer_id BIGINT,
    branch_id BIGINT,
    account_type VARCHAR(30),
    balance DECIMAL(18,2),
    open_date DATE,
    status VARCHAR(30),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (account_id)
);


####################################### branches.csv ######################
CREATE TABLE branches (
    branch_id BIGINT,
    branch_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    opened_date DATE,
    ifsc_code VARCHAR(20),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (branch_id)
);

-- ###################################################################################################################################

-- ################ Load accounts
LOAD DATA LOCAL INFILE "C:/Users/Ram/Downloads/dataset/initial_load/accounts.csv"
INTO TABLE accounts
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS
(
    account_id,
    customer_id,
    branch_id,
    account_type,
    balance,
    open_date,
    status
);

-- #################### Load branchs
LOAD DATA LOCAL INFILE "C:/Users/Ram/Downloads/dataset/initial_load/branches.csv"
INTO TABLE branches
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS
(
    branch_id,
    branch_name,
    city,
    state,
    opened_date,
    ifsc_code
);

-- ############### Load card_transactions
LOAD DATA LOCAL INFILE "C:/Users/Ram/Downloads/dataset/initial_load/card_transactions.csv"
INTO TABLE card_transactions
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS
(
    card_txn_id,
    card_id,
    txn_date,
    merchant_category,
    amount,
    is_fraud
);

-- ################ Load cards
LOAD DATA LOCAL INFILE "C:/Users/Ram/Downloads/dataset/initial_load/cards.csv"
INTO TABLE cards
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS
(
    card_id,
    customer_id,
    account_id,
    card_type,
    issue_date,
    expiry_date,
    credit_limit,
    status
);

-- ###################### Load customers
LOAD DATA LOCAL INFILE "C:/Users/Ram/Downloads/dataset/initial_load/customers.csv"
INTO TABLE customers
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS
(
    customer_id,
    name,
    gender,
    date_of_birth,
    city,
    state,
    phone,
    email,
    occupation,
    annual_income,
    join_date,
    credit_score
);

-- ####################Load employees
LOAD DATA LOCAL INFILE "C:/Users/Ram/Downloads/dataset/initial_load/employees.csv"
INTO TABLE employees
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS
(
    employee_id,
    name,
    branch_id,
    role,
    hire_date,
    salary
);

-- ########################## Load loan_payments
LOAD DATA LOCAL INFILE "C:/Users/Ram/Downloads/dataset/initial_load/loan_payments.csv"
INTO TABLE loan_payments
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS
(
    payment_id,
    loan_id,
    payment_date,
    amount_paid,
    principal_component,
    interest_component,
    late_payment_flag
);

-- ########################### Load loans
LOAD DATA LOCAL INFILE "C:/Users/Ram/Downloads/dataset/initial_load/loans.csv"
INTO TABLE loans
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS
(
    loan_id,
    customer_id,
    branch_id,
    loan_type,
    loan_amount,
    interest_rate,
    term_months,
    start_date,
    status
);

-- ########################### support_tickets
LOAD DATA LOCAL INFILE "C:/Users/Ram/Downloads/dataset/initial_load/support_tickets.csv"
INTO TABLE support_tickets
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS
(
    ticket_id,
    customer_id,
    issue_type,
    date_opened,
    date_resolved,
    status,
    satisfaction_score
);

-- ##################Load Transactions
LOAD DATA LOCAL INFILE "C:/Users/Ram/Downloads/dataset/initial_load/transactions.csv"
INTO TABLE transactions
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS
(
    transaction_id,
    account_id,
    txn_date,
    txn_type,
    amount,
    channel,
    merchant_category
);
