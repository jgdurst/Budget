import pandas as pd
import numpy as np
import os
import pyodbc
import math
import datetime


#Checking/Savings Columns:
# Details (Debit/Credit) --NO
# Posting Date --YES
# Description  --YES
# Amount --YES
# Type (ATM, ACH_Credit/Debit) --NO
# Balance --NO
# Check or Slip # --YES

#Credit Card Columns:
# Transaction Date --NO
# Post Date --YES
# Description --YES
# Category (Chase's guess) --NO
# Type (Payment, Sale, Return) --NO
# Amount --YES

db_file_path = r"C:\Users\jgdur\OneDrive\Budget\Budget_v2.accdb"
connection_string = r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % (db_file_path)




def import_transactions(account, file_name):
    chase_date_parser = lambda x: pd.datetime.strptime(x, '%m/%d/%Y')
    file_path = f'C:\\Users\\jgdur\\OneDrive\\Budget\\Budget Files\\{account} Transactions\\{file_name}'

    if (account == 'Checking' or account == 'Savings'):
        #import checking or savings transactions
        transactions = pd.read_csv(file_path, parse_dates=['Posting Date'], date_parser=chase_date_parser,
                                   usecols=['Posting Date', 'Description', 'Amount', 'Check or Slip #', 'Category'],
                                   keep_default_na=False
        )
        transactions.columns = ['PostingDate', 'Description', 'Amount', 'Check', 'Category']
    else:
        #import credit card transactions
        transactions = pd.read_csv(file_path, parse_dates=['Post Date'], date_parser=chase_date_parser,
                                   usecols=['Post Date', 'Description', 'Amount', 'Category'],
                                   keep_default_na=False
        )
        transactions.columns = ['PostingDate', 'Description', 'Amount', 'Category']
        transactions['Check'] = 0


    transactions['Description'] = transactions['Description'].map(lambda x: " ".join(str(x).split()).replace("'",""))

    try:
        transactions['TransID'] = transactions['PostingDate'].dt.date.map(str) \
                             + '_' \
                             + transactions['Amount'].map('{:0=10.2f}'.format).str.replace('-', 'n') \
                             + '_' \
                             + transactions['Description'].map(lambda x: x.replace(' ', '').replace('_','+'))
    except:
        print(f'Invalid data values in Date, Description, or Amount columns. {len(transactions.index)} rows found.')


    if transactions['TransID'].duplicated().any():
        print('Duplicated transactions found: \n'
              + transactions['TransID'][transactions['TransID'].duplicated() == True])
    else:
        transactions['IsParentTrans'] = transactions.apply(lambda row: '|' in row.Category, axis=1)
        return transactions



def import_allocations(file_name):
    file_path = 'C:\\Users\\jgdur\\Documents\Budget Files\\Allocations\\' + file_name
    allocations = pd.read_csv(file_path)
    expected_cols = {'Category','AllocationYear','AllocationMonth', 'Amount'}
    if (expected_cols.issubset(allocations.columns)):
        return allocations
    else:
        print(f'Expected columns not found in dataset:\n{expected_cols}')



def insert_transactions(account, trans_to_insert, logging=False):
    #connect to database
    conn = pyodbc.connect(connection_string)

    cursor = conn.cursor()

    cursor.execute(
        f'''
        SELECT Account.Account FROM Account
        WHERE Account.Account = '{account}';
        '''
    )
    if not cursor.fetchone():
        print(f'{trans.TransID}\n--\'{account}\' is not a valid Account.')
        return

    cnt_new = 0
    cnt_exist = 0
    cnt_invalid_subtrans = 0
    cnt_invalid_category = 0

    for trans in trans_to_insert.itertuples():
        try:
            check = 'NULL' if math.isnan(trans.Check) else str(trans.Check)
        except:
            check = 'NULL' if not trans.Check else trans.Check
        invalid_subtrans = False
        sum_subtrans_amount = 0
        category_spend_dict = {}

        cursor.execute(
            f'''
            SELECT Category.Category FROM Category
            WHERE Category.Category = '{trans.Category}';
            '''
        )


        if trans.Category and not (trans.IsParentTrans or cursor.fetchone()):
            if logging:
                print('Invalid Category')
            cnt_invalid_category += 1
            print(f'{trans.TransID}\n--\'{trans.Category}\' is not a valid Category.')
            continue


        if trans.IsParentTrans:
            adj_category = ''
            trans_category_list = trans.Category.split('|')
            for val in trans_category_list:
                if (val.strip()):
                    print("'" + val + "'")
                    category = val[:val.find('(')].strip()
                    amount = val[val.find('(') + 1:val.find(')')].strip()
                    cursor.execute(
                        f'''
                        SELECT Category.Category FROM Category
                        WHERE Category.Category = '{category}';
                        '''
                    )
                    if not cursor.fetchone():
                        invalid_subtrans = True
                        break
                    else:
                        sum_subtrans_amount += float(amount)
                        category_spend_dict[category] = amount
            # if (sum_subtrans_amount != trans.Amount):
            #     invalid_subtrans = True
        else:
            adj_category = trans.Category

        if (not invalid_subtrans):
            try:
                if logging:
                    print(f"{trans.TransID}, {trans.PostingDate}, {trans.Description}, {trans.Amount}, '', {check}, {account}, {adj_category}")
                cursor.execute(
                    f'''
                    INSERT INTO Transaction (TransID, TransactionDate, Description, Amount, Memo, Check, Account, Category)
                    VALUES ('{trans.TransID}', '{trans.PostingDate}', '{trans.Description}', {trans.Amount}, '', {check}, '{account}', '{adj_category}');
                    '''
                )
                if logging:
                    print('Success')
                cnt_new += 1
                # print(f'{trans.TransID}\n--Successfully uploaded.')

                for category, amount in category_spend_dict.items():
                    cursor.execute(
                        f'''
                        INSERT INTO SubTransaction (ParentTransID, Category, Amount)
                        VALUES ('{trans.TransID}', '{category}', {amount});
                        '''
                    )

            except pyodbc.IntegrityError:
                if logging:
                    print('Duplicate')
                cnt_exist += 1
                # print(f'{trans.TransID}\n--Record already exists.')
                continue
        else:
            if logging:
                print('Invalid Subtransactions')
            cnt_invalid_subtrans += 1

    conn.commit()

    print(f'Total Records: {len(trans_to_insert.index)}')
    if cnt_new > 0:
        print(f'New records: {cnt_new}')
    if cnt_exist > 0:
        print(f'Duplicates: {cnt_exist}')
    if cnt_invalid_category > 0:
        print(f'Records with Invalid Categories: {cnt_invalid_category}')
    if cnt_invalid_subtrans > 0:
        print(f'Records with Invalid Subtransactions: {cnt_invalid_subtrans}')


def import_insert_transactions(account, file_name, logging=False):
    trans_data = import_transactions(account, file_name)
    insert_transactions(account, trans_data)



def single_allocation(category, year, month, amount, alloc_type):
    conn = pyodbc.connect(connection_string)

    cursor = conn.cursor()

    cursor.execute(
        f'''
        SELECT NULL
        FROM Allocation
        WHERE (
            Allocation.Category = '{category}'
        );
        '''
    )

    if not cursor.fetchone():
        insert_new_year(year)


    if (alloc_type == 'add'):
        alloc_amount_str = f'Allocation.Amount + {amount}'
    elif (alloc_type == 'set'):
        alloc_amount_str = f'{amount}'

    cursor.execute(
        f'''
        UPDATE Allocation 
        SET Allocation.Amount = {alloc_amount_str}
        WHERE (
            Allocation.Category = '{category}' AND
            Allocation.AllocationYear = {year} AND
            Allocation.AllocationMonth = {month}
        );
        '''
    )
    conn.commit()



def mass_allocation(alloc_data, alloc_type):
    for alloc_row in alloc_data.itertuples():
        single_allocation(alloc_row.Category, alloc_row.AllocationYear, alloc_row.AllocationMonth, alloc_row.Amount, alloc_type)



def get_categories():
    conn = pyodbc.connect(connection_string)

    cursor = conn.cursor()

    query = (
        f'''
        SELECT * FROM Category;
        '''
    )

    return pd.read_sql(query, conn)



def get_allocations_by_period(year, month):
    conn = pyodbc.connect(connection_string)

    query = (
        f'''
        SELECT Category.ID, Category.Section, Category.Category, Allocation.Amount
        FROM Allocation 
        LEFT JOIN Category 
        ON Allocation.Category = Category.Category
        WHERE 
        (
            (Allocation.AllocationYear)={year} AND 
            (Allocation.AllocationMonth)={month}
        );
        '''
    )

    return pd.read_sql(query, conn)


def get_data(tablename):
    conn = pyodbc.connect(connection_string)

    query = (
        f'''
        SELECT * FROM {tablename};
        '''
    )

    return pd.read_sql(query, conn)


def get_trans_by_period(year, month):
    conn = pyodbc.connect(connection_string)

    query = (
        f'''
        SELECT * FROM Transaction
        WHERE Transaction.TransactionYear = {year} AND Transaction.TransactionMonth = {month};
        '''
    )

    return pd.read_sql(query, conn)


def get_trans_summ_by_period(year, month):
    conn = pyodbc.connect(connection_string)

    query = (
        f'''
        SELECT Category.Category, Transaction.TransactionMonth, Transaction.TransactionYear, Sum(Transaction.Amount) AS SumAmount
        FROM Category LEFT JOIN [Transaction] ON Category.ID = Transaction.Category
        WHERE Transaction.TransactionMonth = {month} AND Transaction.TransactionYear = {year}
        GROUP BY Category.Category, Transaction.TransactionMonth, Transaction.TransactionYear;
        '''
    )

    return pd.read_sql(query, conn)


def validate_unique_trans(df_validate):
    df_trans = get_data('Transaction')
    for row in df_validate.itertuples():
        if (df_trans['TransID'].isin([row.TransID]).any()):
            print('Record already exists in DB: ' + row.TransID)


def insert_new_year(year):
    conn = pyodbc.connect(connection_string)

    cursor = conn.cursor()

    for i in range(1, 13):
        cursor.execute(
            f'''
            SELECT NULL
            FROM Timeline
            WHERE Timeline.BudgetYear = {year} AND Timeline.BudgetMonth = {i};
            '''
        )
        if not cursor.fetchone():
            cursor.execute(
                f'''
                INSERT INTO Timeline (BudgetYear, BudgetMonth, BudgetMonthDesc)
                VALUES ({year}, {i}, '{datetime.date(year, i, 1).strftime('%b')}');
                '''
            )
        conn.commit()
        insert_allocation_categories(year, i)



def insert_allocation_categories(year, month):
    category_data = get_categories()

    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    for row in category_data.itertuples():
        cursor.execute(
            f'''
            SELECT NULL 
            FROM Allocation
            WHERE (
                Allocation.AllocationYear = {year} AND 
                Allocation.AllocationMonth = {month} AND 
                Allocation.Category = '{row.Category}'
            );
            '''
        )
        if not cursor.fetchone():
            cursor.execute(
                f'''
                INSERT INTO Allocation (Category, Section, AllocationYear, AllocationMonth, Amount)
                VALUES ('{row.Category}', '{row.Section}', {year}, {month}, 0);
                '''
            )

    conn.commit()



def get_current_year():
    return datetime.date.today().year



def get_current_month():
    return datetime.date.today().month



def get_month_name(month):
    return datetime.date(get_current_year(), month, 1).strftime('%b')



if __name__ == '__main__':
    pass

