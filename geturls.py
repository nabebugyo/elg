# geturls
# get urls from single-page and output to a csv-file
# usage
# arg:	target-url
# ret:	0:success
# out:	urllist.csv

#----------
# get argument
#----------
import sys
argvs = sys.argv
if len(argvs) != 2:
	print("USAGE: python geturls.py TARGETURL")
targeturl = argvs[1]
print(targeturl)


#----------
# get html
#----------
import urllib.request

response = urllib.request.urlopen(targeturl)
rescode = response.getcode()

if rescode != 200:
	print("urlopen error code:"+str(rescode))
#else:
#	print("urlopen success code:"+str(rescode))

html = response.read()

source = html.decode("utf-8")
#print(source)


#----------
# extract urls
#----------
urls = []
s1 = source

while 1:
	s1 = s1[s1.find("href"):]
	s1 = s1[s1.find("="):]
	s1 = s1[s1.find("\""):] # change this if url is string variable
	s1 = s1[1:]
	url = s1[:s1.find("\"")]
	print(url[-4:])
	if url == "":
		break
	elif url[-3:] != "htm" and url[-4:] != "html" and url[-1] != "/": # ex. css
		continue
	urls.append(url)

print(urls)


#----------
# output to file
#----------
fw = open("urllist.csv", "w")
for l in urls:
	fw.write(l+",")
fw.close()


retrun 0

