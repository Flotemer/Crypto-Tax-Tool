import Transactions
import pandas as pd
import numpy as np

#Fle Names
input_transactions_file_name = 'Master_Transaction_List.csv'
output_transactions_file_name = "Transactions_Output.csv"
output_receipts_file_name = "Receipts_Output.csv"

#create instance of transactions object
transactions = Transactions.Transactions()

#load csv file into a data frame
df = pd.read_csv(input_transactions_file_name, dtype={
									'transaction_id':str,
									'timestamp':str,
									'platform':str,
									'purchased_currency':str,
									'sold_currency':str,
									'purchased_qty':str,
									'sold_qty:':str,
									'fee_currency':str,
									'fee_qty':str,
									'purchased_currency_spot_price_usd':str,
									'sold_currency_spot_price_usd':str}
				)
#call transactions method to load dataframe with transactions into its contents
transactions.import_csv(df)

#call method to iterate through transactions to create sale receipts with profits/losses
transactions.generate_all_receipts()

#output receipts to a file
transactions.sale_receipts.output_df().to_csv(output_receipts_file_name, sep=',', encoding='utf-8')

#output transactions to a file
transactions.output_df().to_csv(output_transactions_file_name, sep=',', encoding='utf-8')

