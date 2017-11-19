# selecturl
# usage:
# arg: none
# ret: unimplemented
# out: queue to geturls

#----------
# read new urllist
# and join it to parent dataframe
#----------
def read_urllist_from_csv_to_df(parentkey, parentdepth):
	import pandas as pd
	try:
		df = pd.read_csv("urllist.csv")
	except:
		print("./urllist.csv is empty or not existing.")
		return 0
	df = df.T
	df.reset_index(drop=False, inplace=True)
	df.columns = ["url"]
	df["last_scanned_date"] = 0
	df["score"] = 0
	df["depth"] = parentdepth + 1
	df["parent"] = parentkey
	
	# because it doesnt work below
	# df[df.url[-4:] != "html" or df.url[-3:] != "htm"]
	droplist = []
	for key, row in df.iterrows():
		url, date, score, depth, parent = list(row)
		if url[-4:] != "html"\
		 and url[-3:] != "htm"\
		 and url[-1] != "/":
			droplist.append(key)
	df = df.drop(droplist)
	
	if "df_urldb" in locals():
		df_urldb = pd.concat([df_urldb, df])
	else: # in case of a first execution
		df_urldb = df
	
	del df
	return df_urldb


#----------
# queue to geturls
#----------
def queue_to_geturls(targeturl):
	import geturls as g
	import random
	from time import sleep
	sleep(random.randint(1,50)/100)
	g.get_urls(targeturl)
	return 0


#----------
# scan each urls on urldb
#----------
def select_url(df_urldb):
	# get date
	import datetime
	date = datetime.datetime.today()
	#today = print("%s%s%s" % (date.year, date.month, date.day))
	today = str(date.year)+str(date.month)+str(date.day)
	
	# choose a url from db and get cited urls
#	import sys
	import pandas as pd
	for key, row in df_urldb.iterrows():

		# get a record
		url, date, score, depth, parent = list(row)
		if url[:4] != "http":
			continue

		# limitation on searching depth
		MAX_DEPTH = 10
		if depth > MAX_DEPTH:
			continue
		
		# pick a url and scan it
		if date != today:
			targeturl = url
			print("try to scan: " + targeturl)
			queue_to_geturls(targeturl)
			df_urldb.last_scanned_date = today
			new_urldb = read_urllist_from_csv_to_df(key, depth)
			df_urldb = pd.concat([df_urldb, new_urldb])
			df_urldb.reset_index(drop=True, inplace=True)
			return df_urldb, key, 1
	return df_urldb, key, 0


#----------
# update_urldb
#----------
def update_urldb():
#	import pandas as pd
#	df_urldb = pd.DataFrame()
	initial_key = 0
	initial_depth = 0
	df_urldb = read_urllist_from_csv_to_df(initial_key, initial_depth)
	ret = 1
	while ret == 1:
		df_urldb, last_scanned_id, ret = select_url(df_urldb)
		print(df_urldb)
	return 0

	
#----------
# main
#----------
if __name__ == "__main__":
	update_urldb()

