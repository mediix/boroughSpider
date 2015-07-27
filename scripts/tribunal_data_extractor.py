import MySQLdb, re
import urllib2
import requests
from bs4 import BeautifulSoup
from scrapy.item import DictItem, Field
from libextract import extract
from libextract.tabular import parse_html
from lxml.html import open_in_browser

def write(link):
	'''
	link: html link to parse data from
	cookie: retrieved cookie
	returns: writes data into the database
	and downloads the target PDF
	'''
	ln = requests.get(link)
	soup = BeautifulSoup(ln.content) # , 'html.parser')
	table = soup.find('table', {'class': 'lvtFullTable'})

	keys = []
	for th in table.findAll('th'):
		if th.get_text(strip=True) in keys:
			keys.append(str(th.get_text(strip=True)) + '_')
		else:
			keys.append(str(th.get_text(strip=True)).replace(' ', '_').replace('%', 'percentage'))

	items = []
	for tr in table.findAll('tr')[2:]: # skipping first two rows (table bar and header)
		vals = []
		for td in tr.findAll('td'):
			vals.append(td.get_text(strip=True).encode('ascii', 'ignore'))
		dic = dict(zip(keys, vals))
		try:
			dic.update({'Download': str(base_1 + tr.find('a').get('href').replace('../', ''))})
		except:
			dic.update({'Download': 'N/A'})
		items.append(dic)

	con = MySQLdb.connect(user='scraper',
		passwd='12345678',
		db='research_uk',
		host='granweb01',
		charset="utf8",
		use_unicode=True)
	cur = con.cursor()

	cur.execute("SHOW COLUMNS FROM leaseholds")
	cols = cur.fetchall()
	chk = lambda col: re.match(r'^\w+$', col)
	cols = [str(x[0]) for x in cols][1:]
	colnames = ','.join("`%s`" %col if not chk(col) else '%s' %col for col in cols)
	wildcards = ','.join(['%s'] * len(cols))
	missing_items = [2292, 290, 997, 998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034, 1035, 1036, 1037, 1038, 1039, 1040, 1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1060, 1061, 1062, 1063, 1064, 1065, 1066, 1067, 1068, 1069, 1070, 1071, 1072, 1073, 1074, 1075, 1076, 1077, 1078, 1079, 1080, 1081, 1082, 1083, 1084, 1085, 1086, 1087, 1088, 1089, 1090, 1091, 1092, 1093, 1094, 1095, 1097, 1098, 1099, 1100, 1101, 1102, 1103, 1104, 1105, 1106, 1107, 1108, 1109, 1110, 1111, 1112, 1113, 1114, 1115, 1116, 1117, 1118, 1119, 1120, 1121, 1122, 1123, 1124, 1125, 1126, 1127, 1128, 1129, 1130, 1131, 1132, 1133, 1134, 1135, 1136, 1137, 1138, 1139, 1140, 1141, 1142, 1143, 1144, 1145, 1146, 1147, 1148, 1149, 1150, 1151, 1152, 1153, 1154, 1155, 1156, 1157, 1158, 1159, 1160, 1161, 1162, 1163, 1164, 1165, 1166, 1167, 1168, 1169, 1170, 1171, 1172, 1173, 1174, 1175, 1176, 1177, 1178, 1179, 1180, 1181, 1182, 1183, 1184, 1185, 1186, 1187, 1188, 1189, 1190, 1191, 1192, 1193, 1194, 1195, 1196, 1198, 1199, 1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211, 1212, 1213, 1214, 1215, 1216, 1217, 1218, 1219, 1220, 1221, 1222, 1223, 1224, 1225, 1226, 1227, 1228, 1229, 1230, 1231, 1232, 1233, 1234, 1235, 1236, 1237, 1238, 1239, 1240, 1241, 1242, 1243, 1244, 1245, 1246, 1247, 1248, 1249, 1250, 1251, 1252, 1253, 1254, 1255, 1256, 1257, 1258, 1259, 1260, 1261, 1262, 1263, 1264, 1265, 1266, 1267, 1268, 1269, 1270, 1271, 1272, 1273, 1274, 1275, 1276, 1277, 1278, 1279, 1280, 1281, 1282, 1283, 1284, 1285, 1286, 1287, 1288, 1289, 1290, 1291, 1292, 1293, 1294, 1295, 1296, 1463, 1550, 1941, 1942, 1943, 1944, 1945, 1946, 1947, 1948, 1949, 1950, 1951, 1952, 1953, 1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998]

	for it in items:
		data = tuple([it.get(col) for col in cols])
		insert_sql = 'INSERT INTO leasehold (%s) VALUES (%s)' % (colnames, wildcards)

		# check if it.get('Address') exists in addresses table and get address_id
		# insert address into addresses table and get address_id
		# address_id = 1
		# cur.execute('')
		# con.commit()
		data[0] = address_id

		#if data[1] in missing_items:
		print 'insert: {0}\ndata: {1}'.format(insert_sql, data)
		# cur.execute(insert_sql, data)
	# con.commit()
	con.close()

	# for it in items[2:]:
	# 	url = it.get('Download')
	# 	print url
	# 	response = urllib2.urlopen(url)
	# 	file_name = url.split('/')[-1]
	# 	f = open(files_store + file_name, 'wb')
	# 	f.write(response.read())
	# 	f.close()
	# 	f.close()
	# print "Download and write data for %s Completed" % (base+str(link))

if __name__ == "__main__":
	base = 'http://www.lease-advice.org/lvtdecisions/'
	base_1 = 'http://www.lease-advice.org/'
	files_store = '/home/medi/UK_data/Medi/Tribunal/'
	r = requests.get('http://www.lease-advice.org/lvtdecisions/tables.asp?table=3')
	strat = (parse_html,)
	tab = extract(r.content, strategy=strat)
	links = tab.xpath("//p[text()='View decision numbers: ']//@href")
	for link in links[10:11]:
		url = base + link
		print url
		try:
			write(url)
		except:
			pass
