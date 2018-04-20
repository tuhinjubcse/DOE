import os
from datetime import date, timedelta
startDate =	(date.today() - timedelta(3)).strftime('%Y-%m-%d')
endDate = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
os.system('python3 getData2pole.py  '+startDate+'  '+endDate+'  current')