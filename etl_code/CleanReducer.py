#!/usr/bin/env python
import sys
import re

(key, val) = (None, None)

for line in sys.stdin:
	(key, val) = line.strip().split("\t")

	print("%s, %s" % (str(key), str(val)))
