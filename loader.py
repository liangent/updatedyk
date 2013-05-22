#!/usr/bin/env python

from sys import path, argv
from os import getpid
from os.path import join, dirname
from datetime import datetime

CODE_DIR = dirname(__file__)
path.append(CODE_DIR)

ID_STR = '[%s-%d-%s]' % (datetime.utcnow().strftime('%Y%m%d%H%M%S'), getpid(), argv[1])

logf = open(join(CODE_DIR, 'exec.log'), 'a')
print >>logf, ID_STR, 'LOADING',

import updatedyk
import traceback

try:
    if argv[1] == 'main':
        updatedyk.main(error_log=logf)
    elif argv[1] == 'maintenance':
        updatedyk.maintenance()
    else:
        raise Exception('Unknown action: %s' % argv[1])
except Exception:
    print >>logf, 'TRACEBACK'
    print >>logf, traceback.format_exc()
    print >>logf, ID_STR, 'TBEND'
else:
    print >>logf, 'DONE'

logf.close()
