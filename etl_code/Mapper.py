#!/usr/bin/env python
import sys
for line in sys.stdin:
	row = line.strip().split(',')
	print("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (row[-9], row[-8], row[-8][:5], row[-6], row[-5], row[-4], row[-3], row[-2], row[-1]))
