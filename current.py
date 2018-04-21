import os
from datetime import date, timedelta
start = 1
count = 0
while count<5:
	endDate =	(date.today() - timedelta(start)).strftime('%Y-%m-%d')
	startDate = (date.today() - timedelta(start+3)).strftime('%Y-%m-%d')
	start = start+3
	count = count+1
	if start>1:
		os.system('python3 getData1pole.py  '+startDate+'  '+endDate+'  historical')
		os.system('python3 getData3pole.py  '+startDate+'  '+endDate+'  historical')
	os.system('python3 getData2pole.py  '+startDate+'  '+endDate+'  historical')
