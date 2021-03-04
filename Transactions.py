import pandas as pd
import datetime

class Sale_Receipts:
	def __init__(self,contents = []):
		self.contents = contents

	def output_df(self):
		df = pd.DataFrame()
		df['type'] = []; df['timestamp'] = [];  df['sale_id'] = [];  df['currency'] = [];  df['qty_sold'] = [];  df['cost_basis_transaction_id'] = [];  df['cost_basis'] = [];  df['sale_price'] = [];  df['profit_loss'] = [];
		for i in self.contents:
			df = df.append({'type':i.type, 'timestamp':i.timestamp, 'sale_id': i.sale_id, 'currency':i.currency,'qty_sold':i.qty_sold,'cost_basis_transaction_id':i.cost_basis_transaction_id,'cost_basis':i.cost_basis,'profit_loss':i.profit_loss,'sale_price':i.sale_price,}, ignore_index=True)
		return df

class Receipt:
	def __init__(self,timestamp,sale_id,currency,qty_sold,cost_basis_transaction_id,cost_basis,sale_price,profit_loss,type):
		self.timestamp = timestamp
		self.sale_id = sale_id
		self.currency = currency
		self.qty_sold = qty_sold
		self.cost_basis_transaction_id = cost_basis_transaction_id
		self.cost_basis = cost_basis
		self.sale_price = sale_price
		self.profit_loss = profit_loss
		self.type = type
	def print(self):
		print("type: " + str(self.type) + ", "
				+ "timestamp: " + str(self.timestamp) + ", "
				+ "sale_id: " + self.sale_id + "\n" 
				+ "currency: " + self.currency + ", "
				+ "qty_sold: " + str(self.qty_sold) + ", "
				+ "cost_basis_transaction_id: " + self.cost_basis_transaction_id + ", "
				+ "cost_basis: " + str(self.cost_basis) + ", "
				+ "sale_price: " + str(self.sale_price) + "\n"
				+ "profit_loss: " + str(self.profit_loss)
			) 

class Transactions:
	def __init__(self,contents = [],sale_receipts = Sale_Receipts()):
		self.contents = contents
		self.sale_receipts = sale_receipts
	def print_contents(self):
		for i in self.contents:
			i.print()
	def import_csv(self,df):
		df.fillna(0.0, inplace=True)
		rows = df.shape[0] 
		i = 0
		tx_id = ""
		t_stamp = None
		while i < rows:
			#determine tx_id
			
			if df.iloc[i].transaction_id == 0.0:
				tx_id = df.iloc[i].platform +"-"+ df.iloc[i].timestamp
			else:
				tx_id = df.iloc[i].transaction_id

			#convert timestamp
			try:
				t_stamp = datetime.datetime.strptime(df.iloc[i].timestamp , '%Y-%m-%dT%H:%M:%S.%fZ')
			except:
				try:
					t_stamp = datetime.datetime.strptime(df.iloc[i].timestamp , '%Y-%m-%dT%H:%M:%SZ')
				except:
					t_stamp = datetime.datetime.strptime(df.iloc[i].timestamp , '%m/%d/%y %H:%M') 
			a = Transaction(transaction_id = tx_id,
							timestamp = t_stamp, 
							platform = df.iloc[i].platform, 
							purchased_currency = df.iloc[i].purchased_currency,
							sold_currency = df.iloc[i].sold_currency,
							purchased_qty = float(df.iloc[i].purchased_qty),
							sold_qty = float(df.iloc[i].sold_qty),
							fee_currency = df.iloc[i].fee_currency,
							fee_qty = 	float(df.iloc[i].fee_qty),
							purchased_currency_spot_price_usd = float(df.iloc[i].purchased_currency_spot_price_usd),
							sold_currency_spot_price_usd = float(df.iloc[i].sold_currency_spot_price_usd))
			self.add_transaction(a)
			i += 1
	def add_transaction(self,transaction):
		duplicate = False
		for i in self.contents:
			if i.transaction_id == transaction.transaction_id:
				
				duplicate = True
				print("duplicate")
		if duplicate == False: 
			self.contents.append(transaction)
	def return_tx(self,tx_id):
		for i in self.contents:
			if i.transaction_id == tx_id:
				return i
	def generate_all_receipts(self):
		self.contents.sort(key=lambda x: x.timestamp, reverse=False)
		for i in self.contents:
			#generate receipt for sale
			self.generate_receipt_for_transaction(qty_to_allocate = i.sold_qty,currency = i.sold_currency, sale_spot_price = i.sold_currency_spot_price_usd,sale_datetime = i.timestamp, tx_id = i.transaction_id, type = "" )
			#generate receipt for fee if applicable
			if i.fee_currency != i.purchased_currency:
				self.generate_receipt_for_transaction(qty_to_allocate = i.fee_qty,currency = i.fee_currency, sale_spot_price = i.fee_spot_price,sale_datetime = i.timestamp, tx_id = i.transaction_id, type = "Fee Receipt" )
	def generate_receipt_for_transaction(self,qty_to_allocate,currency,sale_spot_price,sale_datetime,tx_id,type):
		if currency not in ["USD", "USDC"]:
			left_to_allocate = qty_to_allocate
			for i in self.contents:	
				if left_to_allocate >0:
					
					if i.purchased_currency == currency and i.timestamp < sale_datetime and i.purchased_qty != i.qty_realized:
						if i.purchased_qty - i.qty_realized >= left_to_allocate:
							n = left_to_allocate
						else:
							n = i.purchased_qty -i.qty_realized

						left_to_allocate -= n
						i.qty_realized += n
						#create receipt to be added to list of receipt

						r = Receipt(timestamp = sale_datetime,
									sale_id=tx_id,
									currency = currency,
									qty_sold = n,
									cost_basis_transaction_id = i.transaction_id,
									cost_basis = i.purchased_currency_spot_price_usd,
									sale_price = sale_spot_price,
									profit_loss = (sale_spot_price-i.purchased_currency_spot_price_usd)*n,
									type = type)
						self.sale_receipts.contents.append(r)
			if left_to_allocate > 0:
				print("ERROR:Unable to create enough receipts to complete purchase for tx id:"+tx_id)
				#in the event that no history is found, error on side of overestimate profits
				n = left_to_allocate
				r = Receipt(timestamp = sale_datetime,
							sale_id=tx_id,
							currency = currency,
							qty_sold = n,
							cost_basis_transaction_id = "No Cost Basis Found",
							cost_basis = 0,
							sale_price = sale_spot_price,
							profit_loss = (sale_spot_price*n),
							type = type)
				self.sale_receipts.contents.append(r)
	def output_df(self):
		df = pd.DataFrame()
		df['transaction_id'] = []; df['timestamp'] = [];  df['platform'] = [];  df['purchased_currency'] = [];  df['sold_currency'] = [];  
		df['purchased_qty'] = [];  df['sold_qty'] = [];  df['fee_currency'] = [];  df['fee_qty'] = [];df['purchased_currency_spot_price_usd'] = [];
		df['sold_currency_spot_price_usd'] = [];df['fee_spot_price'] = [];df['qty_realized'] = [];

		for i in self.contents:
			df = df.append({'transaction_id':str(i.transaction_id),'timestamp':i.timestamp,'platform':i.platform, 'purchased_currency':i.purchased_currency,'sold_currency':i.sold_currency,'purchased_qty':i.purchased_qty,'sold_qty':i.sold_qty,'purchased_qty':i.purchased_qty,'sold_qty':i.sold_qty,'fee_currency':i.fee_currency,'fee_qty':i.fee_qty,'purchased_currency_spot_price_usd':i.purchased_currency_spot_price_usd,'sold_currency_spot_price_usd':i.sold_currency_spot_price_usd,'qty_realized':i.qty_realized,'fee_spot_price':i.fee_spot_price}, ignore_index=True)
		return df

class Transaction:
	def __init__(self,transaction_id,timestamp,platform,purchased_currency,sold_currency,purchased_qty,sold_qty,fee_currency,fee_qty = 0,fee_spot_price = 0,purchased_currency_spot_price_usd = 0,sold_currency_spot_price_usd = 0, qty_realized = 0):
		self.transaction_id = transaction_id
		self.timestamp = timestamp
		self.platform = platform
		self.purchased_currency = purchased_currency
		self.sold_currency = sold_currency
		self.purchased_qty = purchased_qty
		self.sold_qty = sold_qty
		self.fee_currency = fee_currency
		self.fee_qty = fee_qty
		self.purchased_currency_spot_price_usd = purchased_currency_spot_price_usd
		self.sold_currency_spot_price_usd = sold_currency_spot_price_usd
		self.qty_realized = qty_realized
		self.fee_spot_price = fee_spot_price
		self.generate_spot_prices()
	def generate_spot_prices(self):
				#infers spot prices with if at least one usd spot price given

				#infer spot price for purchased currency

				if self.purchased_currency_spot_price_usd == 0 and self.sold_currency in ["USD", "USDC"] :
					self.purchased_currency_spot_price_usd = self.sold_qty/self.purchased_qty
				elif self.purchased_currency_spot_price_usd == 0 and self.purchased_currency in ["USD", "USDC"]:
					self.purchased_currency_spot_price_usd = 1
				elif self.purchased_currency_spot_price_usd == 0 and self.sold_currency_spot_price_usd != 0:

					self.purchased_currency_spot_price_usd = (self.sold_qty/self.purchased_qty)*self.sold_currency_spot_price_usd
				elif self.purchased_currency_spot_price_usd == 0:
					print("ERROR: " + self.transaction_id + ":Spot price required for either purchased or sold currency")

				#infer spot price for sold currency
				if self.sold_currency_spot_price_usd == 0 and self.sold_currency in ["USD", "USDC"]:
					self.sold_currency_spot_price_usd = 1
				elif self.sold_currency_spot_price_usd == 0 and self.purchased_currency in ["USD", "USDC"]:
					self.sold_currency_spot_price_usd = self.purchased_qty/self.sold_qty
				elif self.sold_currency_spot_price_usd == 0 and self.purchased_currency_spot_price_usd != 0:
					self.sold_currency_spot_price_usd = (self.purchased_qty/self.sold_qty)*self.purchased_currency_spot_price_usd
				elif self.sold_currency_spot_price_usd == 0:
					print("ERROR: " + self.transaction_id + ":Spot price required for either purchased or sold currency")

				#infer fee currency spot price
				if self.fee_currency in ["USD", "USDC"]:
					self.fee_spot_price = 1
				elif self.fee_currency == self.sold_currency:
					self.fee_spot_price = self.sold_currency_spot_price_usd
				elif self.fee_currency == self.purchased_currency:
					self.fee_spot_price = self.purchased_currency_spot_price_usd
				else: 
					print(self.fee_currency)
					print("ERROR:fee currency does not match other currencies")
	def print(self):
		print("transaction_id: " + self.transaction_id + ", "
				+ "timestamp: " + str(self.timestamp) + ", "
				+ "platform: " + self.platform + "\n" 
				+ "purchased_currency: " + self.purchased_currency + ", "
				+ "sold_currency: " + self.sold_currency + ", "
				+ "purchased_qty: " + str(self.purchased_qty) + ", "
				+ "sold_qty: " + str(self.sold_qty) + ", "
				+ "purchased_currency_spot_price_usd: " + str(self.purchased_currency_spot_price_usd) + ", "
				+ "sold_currency_spot_price_usd: " + str(self.sold_currency_spot_price_usd) + "\n"
				+ "fee_currency: " + str(self.fee_currency) + ", "
				+ "fee_qty: " + str(self.fee_qty)+ ", "
				+ "fee_spot_price: " +str(self.fee_spot_price) + ", "
				+ "qty_realized: " + str(self.qty_realized)
			) 


