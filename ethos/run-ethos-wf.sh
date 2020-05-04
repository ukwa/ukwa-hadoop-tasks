#!/bin/sh

cd ..
source env.sh
cd -

python ethos_wf.py -r hadoop -o hdfs:///9_processing/warcs2mdx/output-wf.jsonl hdfs:///9_processing/warcs2mdx/output.jsonl
