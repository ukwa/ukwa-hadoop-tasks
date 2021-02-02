#!/bin/sh

export MRJOB_CONF=${PWD}/mrjob.conf

python cdx_job.py --archives '../venv.zip#venv' -r hadoop -o output2.cdx "hdfs:///0_original/fc/opera/heritrix/output/warcs/daily-0600/20150812050010/BL-20150812050021920-00000-25859~opera~8443.warc.gz" "hdfs:///heritrix/output/frequent-npld/20210119131039/warcs/BL-NPLD-WEBRENDER-frequent-npld-20210119131039-20210201012219321-02844-n4o6ljbu.warc.gz"

