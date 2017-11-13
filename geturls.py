# geturls
# get urls from single-page and output to a csv-file
# usage
# arg:	target-url
# ret:	0:success
# out:	urllist.csv

# there is a need to divide into functions
# and rename rightly

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
	if url == "":
		break
	elif url[-3:] != "htm" and url[-4:] != "html" and url[-1] != "/": # ex. css
		continue
	urls.append(url)

#print(urls)


#----------
# output to file
#----------
fw = open("urllist.csv", "w")
for l in urls:
	fw.write(l+",")
fw.close()


#----------
# extract body
#----------

# there is a need to delete any words
# except title, link, and plane text in body
# and i feel like ruby is so easy to do it

s1 = source
s1 = s1[s1.find("<body"):]
body = s1[:s1.find("</body>")]

#print(body)


#----------
# output to file
#----------
fw = open("body.txt", "w")
fw.write(body)
fw.close()


