#!/usr/bin/env python

import sys
import re
import urllib2
import urlparse

rate_regex = re.compile('<h1>([\d\.,\s]+)<\/h1>')
time_date_regex = re.compile('<td>([\d:]+)<br>([\d\.]+)<\/td>')

def die(msg):
	print >> sys.stderr, msg
	sys.exit(1)

def main():
	if (len(sys.argv) != 3):
		die("Start with ./" + sys.argv[0] + " <inputfile> <outputfile>")

	with open(sys.argv[1], 'r') as input_file:
		url = input_file.readline().rstrip()

	response = urllib2.urlopen(url)
	content = response.read()

	rate = rate_regex.search(content)
	if (rate is None):
		rate = "n/a"
	else:
		rate = content[rate.start(1):rate.end(1)].rstrip()

	time_date = time_date_regex.search(content)
	if (time_date is None):
		time = "n/a"
		date = "n/a"
	else:
		time = content[time_date.start(1):time_date.end(1)].rstrip()
		date = content[time_date.start(2):time_date.end(2)].rstrip()

	with open(sys.argv[2], 'w') as output_file:
		output_file.write(rate + '\n')
		output_file.write(date + '\n')
		output_file.write(time + '\n')

if (__name__ == '__main__'):
	main()
