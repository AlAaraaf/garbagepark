import pandas as pd
import os
from datetime import datetime
import re

colnames = ['dpdate','dptime','id','dpid','dpstation','rcid','rcstation','seat','sex','age']

def getmonth(date):
	date = str(date)
	day = int(date[6:])
	if day!=0:
		return int(date[4:6])
	else:
		return int(date[4:6])-1

def getday(date):
	date = str(date)
	day = int(date[6:])
	month = getmonth(date)
	if day == 0:
		if month not in [2,8]:
			return 30+(month%2)
		elif month == 8:
			return 31
		else: return 29
	elif day == 99:
		if month not in [2,8]:
			return 29+(month%2)
		elif month == 8:
			return 30
		else: return 28
	else: return day

def getweekday(date):
	date = str(date)
	month = getmonth(date)
	day = getday(date)
	try:
		time = datetime(2020,month,day).strftime("%w")
	except (ValueError):
		time = 7 #注意7是异常
	return time

def gethour(time):
	return str(time).zfill(6)[:2]

files = os.listdir()
for file in files:
	if file == 'newdata':
		continue
	data = pd.read_csv(file, header = None, names  = colnames)
	data.drop(columns = ['seat'], inplace = True)
	data.dropna(axis = 0, subset = ['dpdate','dptime','id','dpstation','rcstation'], inplace = True)
	data.drop_duplicates(subset = ['dpdate','dptime','id'], inplace = True)

	data.assign(hour = data['dptime'].apply(gethour)
	data['month'] = data['dpdate'].apply(getmonth)
	data['day'] = data['dpdate'].apply(getday)
	data['weekday'] = data['dpdate'].apply(getweekday)
	data['province'] = file.rsplit('-')[0]

	checkpat = re.compile(u'[\u4e00-\u9fa5]')
	data['checkname1'] = data[['dpstation','rcstation']].apply(lambda x: checkpat.search(x['dpstation'])*checkpat.search(x['rcstation']))
	data.groupby['checkname'].count()

	print(file,":",data.shape[0])
	data.to_csv('newdata/'+file, index = False)
  
cd ../Data/newdata
rm data.csv
cat *.csv > data.csv
