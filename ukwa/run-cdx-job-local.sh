#!/bin/sh

export MRJOB_CONF=${PWD}/mrjob.conf

python cdx_job.py --archives '../cdxenv.zip#venv' --read-logs --jobconf mapred.job.name=MRCDXIndexerJob -r local -o output.cdx "BL-20150812050021920-00000-25859~opera~8443.warc.gz"

