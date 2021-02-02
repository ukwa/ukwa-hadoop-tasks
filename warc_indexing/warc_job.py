from datetime import datetime
from urllib.parse import urlparse
from mrjob.job import MRJob
from mrjob.protocol import BytesValueProtocol

class MRWARCScanner(MRJob):

    def jobconf(self):
        return {
            'mapred.job.name': '%s_%s' % ( self.__class__.__name__, datetime.now().isoformat() ),
        } 

    def configure_options(self):
        super(CCJob, self).configure_options()


    # Using mapper_raw means MrJob arranges for a copy of each WARC to be placed where we can get to it:
    def mapper_raw(self, warc_path, warc_uri):
        from warcio.archiveiterator import ArchiveIterator

        warc_hdfs_path = urlparse(warc_uri).path

        with open(warc_path, 'rb') as f:
            ai = ArchiveIterator(f, arc2warc=True)
            for record in ai:
                # General counters:
                self.increment_counter('WARC', 'NUM_RECORDS', 1)
                self.increment_counter('WARC', 'NUM_%s_RECORDS' % record.rec_type.upper(), 1)
                # Specific entries:
                if record.rec_type in ('response', 'revisit') and record.http_headers:
                    sc = record.http_headers.get_statuscode()
                    # Count statuses and yield:
                    self.increment_counter('WARC', 'NUM_STATUS_CODE_%s' % sc, 1)
                    yield record.rec_headers.get_header('WARC-Target-URI', None), sc

    def reducer(self, key, values):
        for value in values:
            yield key, value



if __name__ == '__main__':
    MRWARCScanner.run()

