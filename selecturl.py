# selecturl
# usage:
# arg: none
# ret: unimplemented
# out: queue to geturls

#----------
# read new urllist and join it to parent dataframe
#----------
#fr = open("urllist.csv", "r")
import pandas as pd
df = pd.read_csv("urllist.csv")
df = df.T
df.reset_index(drop=False, inplace=True)
df.columns = ["url"]
df["last_scanned_date"] = 0
df["score"] = 0

if "df_url" in locals():
	df_url = pd.concat([df_url, df])
else:
	df_url = df

#print(df)

del df


#----------
# select url
#----------
for key, row in df_url.iterrows():
	url, date, score = list(row)
	print(url, date)

#----------
# queue to geturls
#----------

