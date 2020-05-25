
import csv
import datetime
import re
import subprocess
import sqlite3
import time

def main():
	find_out = subprocess.check_output(["find",".","-name","*.csv"])

	csv_name_p = re.compile("([0-9]{4})([0-9]{2})([0-9]{2})_OData")

	files = {}
	for line in find_out.split():
		assert(line.endswith(".csv"))
		m = csv_name_p.search(line)
		if m is None:
			print("Uhoh no date match: ", line)
			assert(False)
		dt = datetime.datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
		if dt in files:
			print("Uhoh, duplcate: ", dt)
			assert(False)
		files[dt] = line

	files_sorted = sorted(files)
	min_date = files_sorted[0]
	max_date = files_sorted[len(files_sorted)-1]

	day_count = (max_date - min_date).days + 1
	missing_dates = 0
	for single_date in (min_date + datetime.timedelta(n) for n in range(day_count)):
		if single_date not in files:
			missing_dates = missing_dates + 1

	print("NOTE: Missing {m} days (weekends/holidays?)".format(m=missing_dates))

	with sqlite3.connect("./options.db") as db:
		# create table prices (date INTEGER, symbol varchar(64), expiry INTEGER, askprice REAL, asksize REAL, bidprice REAL, bidsize REAL, lastprice REAL, putcall varchar(16), strike REAL, volume REAL, openinterest REAL, underlyingprice REAL, optionkey varchar(64));
		# (julianday('2004-01-10') - 2440587.5)*86400.0

		# print("Clearing DB...")
		# cur = db.cursor()
		# cur.execute("delete from prices", ())
		# db.commit()

		date_p = re.compile("([0-9]{4})-([0-9]{2})-([0-9]{2})")

#		for date, file in files.items():
		# In order to understand progres
		for date in files_sorted:
			if date < datetime.datetime(2019,6,3):
				continue

			file = files[date]

			with open(file) as csvfile:
				print("Importing {f}...".format(f=file))
				reader = csv.reader(csvfile)
				headers = {}
				first = True
				lineno = 0
				for row in reader:
					lineno = lineno + 1
					# Header row
					if first:
						for h in range(0, len(row)):
							headers[row[h]] = h
						first = False
					else:
						def get_col(names): 
							for name in names:
								if name in headers:
									return row[headers[name]]
							assert(False)
						def udate(d):
							return time.mktime(d.timetuple())

						data_date = get_col(["DataDate"])
						m = date_p.match(data_date)
						assert(m is not None)
						date_row = datetime.datetime(int(m.group(1)),int(m.group(2)),int(m.group(3)))
						assert(date == date_row)

						exp_date_str = get_col(["xdate", "ExpirationDate"])
						m = date_p.match(data_date)
						assert(m is not None)
						exp_date = datetime.datetime(int(m.group(1)),int(m.group(2)),int(m.group(3)))

						sql = """INSERT INTO prices 
(date, symbol, expiry, askprice, asksize, bidprice, bidsize, lastprice, 
putcall, strike, volume, openinterest, underlyingprice, optionkey)

VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
						cur = db.cursor()
						if "undersymbol" in headers:
							root = row[headers["undersymbol"]]
							under = row[headers["rootsymbol"]]
							if(root != under):
								print("symbol mismatch at line {l} {r} != {u}".format(l=lineno, r=root, u=under))

#optionkey,ask,askz,bid,bidz,last,put_call,strikeprice,undersymbol,rootsymbol,vol,xdate,openinterest,UnderlyingPrice,DataDate

						params = (udate(date),\
								  get_col(["rootsymbol", "Symbol"]),\
								  udate(exp_date),\
								  get_col(["ask", "AskPrice"]),\
								  get_col(["askz", "AskSize"]),\
								  get_col(["bid", "BidPrice"]),\
								  get_col(["bidz", "BidSize"]),\
								  get_col(["last", "LastPrice"]),\
								  get_col(["put_call", "PutCall"]),\
								  get_col(["strikeprice", "StrikePrice"]),\
								  get_col(["vol", "Volume"]),\
								  get_col(["openinterest","OpenInterest"]),\
								  get_col(["UnderlyingPrice"]),\
								  get_col(["optionkey","OptionKey"]))
						cur.execute(sql, params)
				db.commit()

if __name__ == '__main__':
    main()

