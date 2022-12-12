
environment = 'PROD'
# environment = input('Please define the environment: ')
print('The environment is: ' + environment)

while(True):
	import datetime
	import time
	a = datetime.datetime.now()
	dotw = a.weekday()
	if environment == 'PROD':
		if dotw < 4 or dotw == 6:
			b = datetime.datetime.now().replace(hour=7, minute=00, second=0, microsecond=0) + datetime.timedelta(days=1)
			sleepamt = (b-a).total_seconds()
		elif dotw == 4:
			b = datetime.datetime.now().replace(hour=7, minute=00, second=0, microsecond=0) + datetime.timedelta(days=3)
			sleepamt = (b-a).total_seconds()
		else:
			b = datetime.datetime.now().replace(hour=7, minute=00, second=0, microsecond=0) + datetime.timedelta(days=2)
			sleepamt = (b-a).total_seconds()			
		print('Current time is:')
		print(a)
		print('Business day scheduled for:')
		print(b)
	else:
		sleepamt = 1
		print('Current time is:')
		print(a)
		print('Business day scheduled for:')
		b = 'NOW - TEST'
		print(b)
	if sleepamt > 0:
		time.sleep(sleepamt)
		print('run YahooDownload_PROD.py')
		exec(open("YahooDownload_PROD.py").read())
		print('YahooDownload_PROD.py completed.')
		print('run Ichimoku_PROD.py')
		exec(open("Ichimoku_PROD.py").read())
		print('Ichimoku_PROD.py completed.')
		print('run OpenInsiderMaster_PROD.py')
		exec(open("OpenInsiderMaster_PROD.py").read())
		print('OpenInsiderMaster_PROD.py completed.')
		if environment != 'PROD':
			exit()
