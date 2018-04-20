import pymssql
from datetime import date, timedelta
import mysql.connector
from mysql.connector import errorcode
import time
import sys

a = []
for line in open('3pole.txt'):
	a.append(line.strip())

rangeMap = {}
aptToSourceID = {}
SourceIDToQuantityID = {}
for line in open('sourceQuantity3pole.txt','r'):
	line = line.strip().split('\t')
	if line[1] not in aptToSourceID:
		aptToSourceID[line[1]]= line[0]
	if line[0] not in SourceIDToQuantityID:
		SourceIDToQuantityID[line[0]] = [(line[2],line[3])]
	else:
		SourceIDToQuantityID[line[0]].append((line[2],line[3]))
	if line[0] not in rangeMap:
		rangeMap[line[0]] = [int(line[2])]
	else:
		rangeMap[line[0]].append(int(line[2]))

count = 0
x = []
if sys.argv[3] == 'historical':
	startDate = sys.argv[1]+' '+' 00:00:00.0000000'
else:
	startDate = sys.argv[1]+' '+' 12:00:00.0000000'
endDate = sys.argv[2]+' '+' 00:00:00.0000000'
query = "select * from dbo.DataLog2 where TimestampUTC >= '"+ startDate+ "' and TimestampUTC < '"+ endDate + "' and SourceID ="
query1 = query
for apt in aptToSourceID:
	x.append(aptToSourceID[apt])


mul = 0

conn = pymssql.connect(server='129.236.255.148\ION',user = '129.236.255.148\AzureRemoteAccess' ,password = 'Sa3424', database = 'master')
cursor = conn.cursor()
cursor.execute('use ION_Data')

results = []
f = open('3poleData.txt','w')
while count<len(x):
	sourceID = ''
	while mul<1 and count<len(x):
		sourceID = x[count]
		query1 = query1+sourceID+" and QuantityID in ("
		count = count+1
		mul=mul+1
	if mul==1 or count == len(x):
		mul = 0
		a = query1
		for i in rangeMap[sourceID]:
			a = a + str(i)+","
		a = a[:-1]+")"
		print(a)
		cursor.execute(a)
		row = cursor.fetchall()
		results.append(row)
		print('Complete batch of 1')
		print('Count is',count)
		mul = 0
		query1 = query
conn.close()

for res in results:
	for r in res:
		f.write(str(r[1])+'\t'+str(r[2])+'\t'+str(r[3])+'\t'+str(r[4])+'\n')


quantityName = {}
aptName = {}
for line in open('sourceQuantity3pole.txt','r'):
	line = line.strip().split('\t')
	if 'A052053' in line[1] or 'A088089' in line[1] or 'A080081' in line[1] or 'A063064' in line[1]:
		if line[2] not in quantityName:
			flag = 'Ph1'
			if 'kWh' in line[3]:
				flag = 'Total'
			if 'LineFreq' in line[3]:
				flag = ''
			quantityName[line[2]] = flag+line[3].split('_')[1]
			aptName[line[2]] = line[3].split('_')[0]
	else:
		if line[2] not in quantityName:
			if 'kWh' in line[3]:
				flag = 'Total'
			else:
				flag = ''
			quantityName[line[2]] = flag+line[3].split('_')[1]
			aptName[line[2]] = line[3].split('_')[0]


config = {
  'host':'mfred-doe.mysql.database.azure.com',
  'user':'MFRED@mfred-doe',
  'password':'Benefit2018',
  'database':'mfred'
}

# Construct connection string
try:
   conn = mysql.connector.connect(**config)
   print("Connection established")
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with the user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
else:
  cursor = conn.cursor()

cursor.execute('use mfred')
start_time = time.time()
count = 0
s1 = ''
s2 = ''
s3= ''
s4 = ''
s5 = ''
s6 =''
s7 = ''

for line in open('3poleData.txt','r'):
	if line.strip().split('\t')[1] in ['239','254','233','229']:
		if count%100000==0:
			if s1.endswith('values')==False and s1!='':
				s1 = s1[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1Amps=VALUES(Ph1Amps),timelocal=VALUES(timelocal)"
				cursor.execute(s1)
				conn.commit()
			if s2.endswith('values')==False and s2!='':
				s2 = s2[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2Amps=VALUES(Ph2Amps),timelocal=VALUES(timelocal)"
				cursor.execute(s2)
				conn.commit()
			if s3.endswith('values')==False and s3!='':
				s3 = s3[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1kVAR=VALUES(Ph1kVAR),timelocal=VALUES(timelocal)"
				cursor.execute(s3)
				conn.commit()
			if s4.endswith('values')==False and s4!='':
				s4 = s4[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2kVAR=VALUES(Ph2kVAR),timelocal=VALUES(timelocal)"
				cursor.execute(s4)
				conn.commit()
			if s5.endswith('values')==False and s5!='':
				s5 = s5[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1kW=VALUES(Ph1kW),timelocal=VALUES(timelocal)"
				cursor.execute(s5)
				conn.commit()
			if s6.endswith('values')==False and s6!='':
				s6 = s6[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2kW=VALUES(Ph2kW),timelocal=VALUES(timelocal)"
				cursor.execute(s6)
				conn.commit()
			if s7.endswith('values')==False and s7!='':
				s7 = s7[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), TotalkWh=VALUES(TotalkWh),timelocal=VALUES(timelocal)"
				cursor.execute(s7)
				conn.commit()
			s1 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph1Amps"+",timelocal) values"
			s2 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph2Amps"+",timelocal) values"
			s3 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph1kVAR"+",timelocal) values"
			s4 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph2kVAR"+",timelocal) values"
			s5 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph1kW"+",timelocal) values"
			s6 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph2kW"+",timelocal) values"
			s7 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"TotalkWh"+",timelocal) values"
			print('100K INSERTED, Batch',count)
		line = line.strip().split('\t')
		apartment = aptName[line[2]]
		colname =  quantityName[line[2]]
		colvalue = line[0]
		date = line[3]
		if colname == 'Ph1Amps':
			s1 = s1+ "('"+apartment+"','2Pole',"+colvalue+",'"+date+"') ,"
		elif colname == 'Ph2Amps':
			s2 = s2 + "('"+apartment+"','2Pole',"+colvalue+",'"+date+"') ,"
		if colname == 'Ph1kVAR':
			s3 = s3 + "('"+apartment+"','2Pole',"+colvalue+",'"+date+"') ,"
		elif colname == 'Ph2kVAR':
			s4 = s4 + "('"+apartment+"','2Pole',"+colvalue+",'"+date+"') ,"
		if colname == 'Ph1kW':
			s5 = s5 + "('"+apartment+"','2Pole',"+colvalue+",'"+date+"') ,"
		elif colname == 'Ph2kW':
			s6 = s6 + "('"+apartment+"','2Pole',"+colvalue+",'"+date+"') ,"
		elif colname == 'TotalkWh':
			s7 = s7 + "('"+apartment+"','2Pole',"+colvalue+",'"+date+"') ,"
		count = count+1

if s1.endswith('values')==False and s1!='':
	s1 = s1[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1Amps=VALUES(Ph1Amps),timelocal=VALUES(timelocal)"
	cursor.execute(s1)
	conn.commit()
if s2.endswith('values')==False and s2!='':
	s2 = s2[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2Amps=VALUES(Ph2Amps),timelocal=VALUES(timelocal)"
	cursor.execute(s2)
	conn.commit()
if s3.endswith('values')==False and s3!='':
	s3 = s3[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1kVAR=VALUES(Ph1kVAR),timelocal=VALUES(timelocal)"
	cursor.execute(s3)
	conn.commit()
if s4.endswith('values')==False and s4!='':
	s4 = s4[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2kVAR=VALUES(Ph2kVAR),timelocal=VALUES(timelocal)"
	cursor.execute(s4)
	conn.commit()
if s5.endswith('values')==False and s5!='':
	s5 = s5[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1kW=VALUES(Ph1kW),timelocal=VALUES(timelocal)"
	cursor.execute(s5)
	conn.commit()
if s6.endswith('values')==False and s6!='':
	s6 = s6[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2kW=VALUES(Ph2kW),timelocal=VALUES(timelocal)"
	cursor.execute(s6)
	conn.commit()
if s7.endswith('values')==False and s7!='':
	s7 = s7[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), TotalkWh=VALUES(TotalkWh),timelocal=VALUES(timelocal)"
	cursor.execute(s7)
	conn.commit()


print("MYSQL Took --- %s seconds ---" % (time.time() - start_time))


count = 0
s1 = ''
s2 = ''
s3= ''
s4 = ''
s5 = ''
s6 =''
s7 = ''
s8 = ''
s9 = ''
s10 = ''

for line in open('3poleData.txt','r'):
	if not line.strip().split('\t')[1] in ['239','254','233','229']:
		if count%100000==0:
			if s1.endswith('values')==False and s1!='':
				s1 = s1[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1Amps=VALUES(Ph1Amps),timelocal=VALUES(timelocal)"
				cursor.execute(s1)
				conn.commit()
			if s2.endswith('values')==False and s2!='':
				s2 = s2[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2Amps=VALUES(Ph2Amps),timelocal=VALUES(timelocal)"
				cursor.execute(s2)
				conn.commit()
			if s3.endswith('values')==False and s3!='':
				s3 = s3[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph3Amps=VALUES(Ph3Amps),timelocal=VALUES(timelocal)"
				cursor.execute(s3)
				conn.commit()
			if s4.endswith('values')==False and s4!='':
				s4 = s4[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1kVAR=VALUES(Ph1kVAR),timelocal=VALUES(timelocal)"
				cursor.execute(s4)
				conn.commit()
			if s5.endswith('values')==False and s5!='':
				s5 = s5[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2kVAR=VALUES(Ph2kVAR),timelocal=VALUES(timelocal)"
				cursor.execute(s5)
				conn.commit()
			if s6.endswith('values')==False and s6!='':
				s6 = s6[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph3kVAR=VALUES(Ph3kVAR),timelocal=VALUES(timelocal)"
				cursor.execute(s6)
				conn.commit()
			if s7.endswith('values')==False and s7!='':
				s7 = s7[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1kW=VALUES(Ph1kW),timelocal=VALUES(timelocal)"
				cursor.execute(s7)
				conn.commit()
			if s8.endswith('values')==False and s8!='':
				s8 = s8[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2kW=VALUES(Ph2kW),timelocal=VALUES(timelocal)"
				cursor.execute(s8)
				conn.commit()
			if s9.endswith('values')==False and s9!='':
				s9 = s9[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph3kW=VALUES(Ph3kW),timelocal=VALUES(timelocal)"
				cursor.execute(s9)
				conn.commit()
			if s10.endswith('values')==False and s10!='':
				s10 = s10[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), TotalkWh=VALUES(TotalkWh),timelocal=VALUES(timelocal)"
				cursor.execute(s10)
				conn.commit()
			s1 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph1Amps"+",timelocal) values"
			s2 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph2Amps"+",timelocal) values"
			s3 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph3Amps"+",timelocal) values"
			s4 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph1kVAR"+",timelocal) values"
			s5 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph2kVAR"+",timelocal) values"
			s6 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph3kVAR"+",timelocal) values"
			s7 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph1kW"+",timelocal) values"
			s8 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph2kW"+",timelocal) values"
			s9 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"Ph3kW"+",timelocal) values"
			s10 = "INSERT into METER_READINGS (apartment_id, column_structure, "+"TotalkWh"+",timelocal) values"
			print('100K INSERTED, Batch',count)
		line = line.strip().split('\t')
		apartment = aptName[line[2]]
		colname =  quantityName[line[2]]
		colvalue = line[0]
		date = line[3]
		if colname == 'Ph1Amps':
			s1 = s1+ "('"+apartment+"','3Pole',"+colvalue+",'"+date+"') ,"
		elif colname == 'Ph2Amps':
			s2 = s2 + "('"+apartment+"','3Pole',"+colvalue+",'"+date+"') ,"
		elif colname == 'Ph3Amps':
			s3 = s3 + "('"+apartment+"','3Pole',"+colvalue+",'"+date+"') ,"
		if colname == 'Ph1kVAR':
			s4 = s4 + "('"+apartment+"','3Pole',"+colvalue+",'"+date+"') ,"
		elif colname == 'Ph2kVAR':
			s5 = s5 + "('"+apartment+"','3Pole',"+colvalue+",'"+date+"') ,"
		elif colname == 'Ph3kVAR':
			s6 = s6 + "('"+apartment+"','3Pole',"+colvalue+",'"+date+"') ,"
		if colname == 'Ph1kW':
			s7 = s7 + "('"+apartment+"','3Pole',"+colvalue+",'"+date+"') ,"
		elif colname == 'Ph2kW':
			s8 = s8 + "('"+apartment+"','3Pole',"+colvalue+",'"+date+"') ,"
		elif colname == 'Ph3kW':
			s9 = s9 + "('"+apartment+"','3Pole',"+colvalue+",'"+date+"') ,"
		elif colname == 'TotalkWh':
			s10 = s10 + "('"+apartment+"','3Pole',"+colvalue+",'"+date+"') ,"
		count = count+1

if s1.endswith('values')==False and s1!='':
	s1 = s1[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1Amps=VALUES(Ph1Amps),timelocal=VALUES(timelocal)"
	cursor.execute(s1)
	conn.commit()
if s2.endswith('values')==False and s2!='':
	s2 = s2[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2Amps=VALUES(Ph2Amps),timelocal=VALUES(timelocal)"
	cursor.execute(s2)
	conn.commit()
if s3.endswith('values')==False and s3!='':
	s3 = s3[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph3Amps=VALUES(Ph3Amps),timelocal=VALUES(timelocal)"
	cursor.execute(s3)
	conn.commit()
if s4.endswith('values')==False and s4!='':
	s4 = s4[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1kVAR=VALUES(Ph1kVAR),timelocal=VALUES(timelocal)"
	cursor.execute(s4)
	conn.commit()
if s5.endswith('values')==False and s5!='':
	s5 = s5[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2kVAR=VALUES(Ph2kVAR),timelocal=VALUES(timelocal)"
	cursor.execute(s5)
	conn.commit()
if s6.endswith('values')==False and s6!='':
	s6 = s6[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph3kVAR=VALUES(Ph3kVAR),timelocal=VALUES(timelocal)"
	cursor.execute(s6)
	conn.commit()
if s7.endswith('values')==False and s7!='':
	s7 = s7[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph1kW=VALUES(Ph1kW),timelocal=VALUES(timelocal)"
	cursor.execute(s7)
	conn.commit()
if s8.endswith('values')==False and s8!='':
	s8 = s8[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph2kW=VALUES(Ph2kW),timelocal=VALUES(timelocal)"
	cursor.execute(s8)
	conn.commit()
if s9.endswith('values')==False and s9!='':
	s9 = s9[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), Ph3kW=VALUES(Ph3kW),timelocal=VALUES(timelocal)"
	cursor.execute(s9)
	conn.commit()
if s10.endswith('values')==False and s10!='':
	s10 = s10[:-1]+ "ON DUPLICATE KEY UPDATE apartment_id=VALUES(apartment_id), column_structure=VALUES(column_structure), TotalkWh=VALUES(TotalkWh),timelocal=VALUES(timelocal)"
	cursor.execute(s10)
	conn.commit()
cursor.close()
conn.close()

print("MYSQL Took --- %s seconds ---" % (time.time() - start_time))




