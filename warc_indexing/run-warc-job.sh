#!/bin/sh

export MRJOB_CONF=${PWD}/mrjob.conf

python warc_job.py --archives '../venv.zip#venv' -r hadoop -o output.warced "hdfs:///0_original/fc/opera/heritrix/output/warcs/daily-0600/20150812050010/BL-20150812050021920-00000-25859~opera~8443.warc.gz"

