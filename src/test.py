from BeautifulSoup import BeautifulSoup
import urllib,string,csv,sys,os
from string import replace

date_s = '&date1=01/01/08'
date_f = '&date=11/10/08'
fx_url = 'http://www.oanda.com/convert/fxhistory?date_fmt=us'
fx_url_end = '&lang=en&margin_fixed=0&format=CSV&redirected=1'
cur1,cur2 = 'USD','AUD'
fx_url = fx_url + date_f + date_s + '&exch=' + cur1 +'&exch2=' + cur1
fx_url = fx_url +'&expr=' + cur2 +  '&expr2=' + cur2 + fx_url_end
data = urllib.urlopen(fx_url).read()
soup = BeautifulSoup(data)
data = str(soup.findAll('pre', limit=1))
data = replace(data,'[<pre>','')
data = replace(data,'</pre>]','')
file_location = './'
file_name = file_location + 'usd_aus.csv'
file = open(file_name,"w")
file.write(data)
file.close()
