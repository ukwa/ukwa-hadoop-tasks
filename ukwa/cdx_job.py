from datetime import datetime
from mrjob.job import MRJob
from urllib.parse import urlparse

class MRCDXIndexer(MRJob):

    def jobconf(self):
        return {
            'mapred.job.name': '%s_%s' % ( self.__class__.__name__, datetime.now().isoformat() ),
            'mapred.compress.map.output':'true',
            'mapred.output.compress': 'true',
            'mapred.output.compression.codec': 'org.apache.hadoop.io.compress.GzipCodec',
        }

    # Sort all the values on each Reducer, so the last event wins in the case of timestamp collisions:`
    SORT_VALUES = True

    # Using mapper_raw means MrJob arranges for a copy of each WARC to be placed where we can get to it:
    # (This breaks data locality, but streaming through large files is not performant because they get read into memory)
    # (A FileInputFormat that could reliably split block GZip files would be the only workable fix)
    # (But TBH this is pretty fast as it is)
    def mapper_raw(self, warc_path, warc_uri):
        from cdxj_indexer.main import CDX11Indexer

        cdx_file = 'index.cdx'

        cdx11 = CDX11Indexer(inputs=[warc_path], output=cdx_file, cdx11=True, post_append=True)
        cdx11.process_all()

        warc_path = urlparse(warc_uri).path

        self.set_status('cdxj_indexer of %s complete.' % warc_path)

        with open(cdx_file) as f:
            for line in f:
                line = line.strip()
                parts = line.split(" ")
                # Skip header:
                if parts[0] == 'CDX':
                    continue
                # Count the lines:
                self.increment_counter('CDX', 'CDX_LINES', 1)
                # Replace `warc_path` with proper HDFS path:
                parts[10] = warc_path
                # Key on host to distribute load:
                host_surt = parts[0].split(")", 1)[0]
                # Reconstruct the CDX line and yield:
                yield host_surt, " ".join(parts)

    def reducer(self, key, values):
        for value in values:
            yield key, value



if __name__ == '__main__':
    MRCDXIndexer.run()

