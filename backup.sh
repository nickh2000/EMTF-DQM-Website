#!/bin/sh
semanage fcontext -a -t bin_t '/root/backup.sh'
restorecon -Fv /root/backup.sh
setenforce 0
export CACHE=/root/keys
export CACERT=/root/keys/CERN_Root_CA.crt
export PUBLIC_KEY=/root/keys/usercert.pem
export PRIVATE_KEY=/root/keys/userkey.key
source /root/root/bin/thisroot.sh
/root/csctiming/csctimingenv/bin/gunicorn --chdir=/root/csctiming/ --workers 3 --bind unix:csctiming.sock -m 007 wsgi:app --timeout 0

