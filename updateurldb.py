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
		return 0, -1
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
	return df_urldb, 0


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
	today = int(str(date.year)+str(date.month)+str(date.day))
	
	# choose a url from db and get cited urls
	import pandas as pd
	for key, row in df_urldb.iterrows():

		# get a record
		url, date, score, depth, parent = list(row)
		if url[:4] != "http":
			continue

		# Debug: limitation on searching depth
		MAX_DEPTH = 3
		if depth > MAX_DEPTH:
			print("Debug: excess of MAX_DEPTH")
			continue

		# pick a url and scan it
		if date != today:
			targeturl = url
			
			# skip duplicated urls
			if df_urldb.url.isin([url]).any() == True:
				latest = max(df_urldb[df_urldb.url.isin([url])]["last_scanned_date"])
				if latest == today:
					print("skip scanned url: " + targeturl)
					df_urldb["last_scanned_date"][key] = today
					df_urldb["depth"][key] = df_urldb["depth"][parent] + 1
					continue
			
			print("try to scan: " + targeturl)
			queue_to_geturls(targeturl)
			df_urldb["last_scanned_date"][key] = today
			new_urldb, ret = read_urllist_from_csv_to_df(key, depth)
			if ret == -1:
				continue
			df_urldb = pd.concat([df_urldb, new_urldb])
#			df_urldb = df_urldb.drop_duplicates(["url"],keep="first")
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
	df_urldb, ret = read_urllist_from_csv_to_df(initial_key, initial_depth)
	ret = 1
	while ret == 1:
		
		# Debug: limitation on url records
		MAX_LENGTH = 20000
		if len(df_urldb) > MAX_LENGTH:
			print("Debug: excess of MAX_LENGTH")
			break

		df_urldb, last_scanned_id, ret = select_url(df_urldb)
		print(df_urldb)
	return df_urldb, 0

	
#----------
# main
#----------
if __name__ == "__main__":
	from cassandra.cluster import Cluster

	host = ["cassandra"]
	keyspace = "elgoog"

	cluster = Cluster(host)
	session = cluster.connect()

	df_urldb, drop = update_urldb()
	df_urldb.to_csv("urldb_out.csv", index=True)
	for i in range(len(df_urldb.index)):
		this_list=df_urldb.loc[i].tolist()
		this_cql="INSERT INTO elgoog.pagetable (id,url,last_scanned_date,score,depth,parent) VALUES ("
		this_cql = this_cql+str(i)+",'"+this_list[0]+"',"+str(this_list[1])+","+str(this_list[2])+","+str(this_list[3])+","+str(this_list[4])+")"
		#print(this_cql)
		session.execute(this_cql)
