from mrjob.job import MRJob
from mrjob.protocol import BytesValueProtocol
from warcio.archiveiterator import ArchiveIterator

class MRCDXIndexer(MRJob):

    #
    # Using this custom input format, rather than 'mapred.min.split.size' as
    # The default TextInputFormat auto-gunzips and leaves only the first record.
    #
    HADOOP_INPUT_FORMAT = 'uk.bl.wa.hadoop.mapred.UnsplittableInputFileFormat'

    LIBJARS = [ "../jars/warc-hadoop-recordreaders-3.2.0-SNAPSHOT-job.jar" ]

    SORT_VALUES = True

    JOBCONF = {
        'mapred.job.name': 'MRWARCProcessorJob',
        #'mapreduce.map.input.length': str(1_000_000_000_000),
        #'mapred.min.split.size': str(1_000_000_000_000) # 1TB split size to prevent files being split, but Gunzipping goes wrong
    } 

    INPUT_PROTOCOL = BytesValueProtocol
   
    def configure_options(self):
        super(CCJob, self).configure_options()
        # Pass through the runner so we can adapt:
        self.pass_through_option('--runner')
        self.pass_through_option('-r')

    def _wrap_protocols(self, step_num, step_type):
        '''Override the default implementation to directly parse stdin as a WARC.'''
        read_lines, write_line = super()._wrap_protocols(step_num, step_type)

        def read_records():
            paths = self.options.args or ['-']
            
            self.set_status( 'CHECK read_records %s' % paths)

            for path in paths:
                if path == '-':
                    # On Hadoop, read up to the first \t to get the path (passed in by UnsplittableInputFileFormat):
                    if self.options.runner == 'inline':
                        key = path
                    else:
                        key = ""
                        while True:
                            char = self.stdin.read(1)
                            if char == b'\t':
                                break
                            key += char.decode("utf-8")
                    # Then read the file:
                    #print("CHECK KEY %s" % key, file=self.stderr)
                    ai = ArchiveIterator(self.stdin, arc2warc=True)
                    for record in ai:
                        self.increment_counter('WARC', 'NUM_RECORDS', 1)
                        self.increment_counter('WARC', 'NUM_%s_RECORDS' % record.rec_type.upper(), 1)
                        yield "%s@%s" % (key, ai.offset), record
                else:
                    with open(path, 'rb') as stream:
                        ai = ArchiveIterator(stream, arc2warc=True)
                        for record in ai:
                            self.increment_counter('WARC', 'NUM_RECORDS', 1)
                            self.increment_counter('WARC', 'NUM_%s_RECORDS' % record.rec_type.upper(), 1)
                            yield "%s@%s" % (path, ai.offset), record

        # If this is a mapper, parse to records:
        if step_type == 'mapper':
            return read_records, write_line
        else:
            return read_lines, write_line

    def mapper(self, key, record):
        if record.rec_type in ('response', 'revisit') and record.http_headers:
            sc = record.http_headers.get_statuscode()
            # Count and return:
            self.increment_counter('WARC', 'NUM_STATUS_CODE_%s' % sc, 1)
            yield record.rec_headers.get_header('WARC-Target-URI', None), sc

    def reducer(self, key, values):
        for value in values:
            yield key, value



if __name__ == '__main__':
    MRCDXIndexer.run()

