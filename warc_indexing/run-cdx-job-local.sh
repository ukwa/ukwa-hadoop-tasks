#!/bin/sh

export MRJOB_CONF=${PWD}/mrjob.conf

python cdx_job.py --cdx-endpoint http://cdx.dapi.wa.bl.uk/mrjob -r local -o output.cdx "BL-20150812050021920-00000-25859~opera~8443.warc.gz" "BL-NPLD-WEBRENDER-frequent-npld-20210119131039-20210201012219321-02844-n4o6ljbu.warc.gz"

