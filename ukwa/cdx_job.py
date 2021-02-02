from mrjob.job import MRJob
from warcio.archiveiterator import ArchiveIterator

class MRCDXIndexerCP(MRJob):

    def mapper_raw(self, warc_path, warc_uri):
        from cdxj_indexer.main import CDX11Indexer

        cdx_file = 'index.cdx'

        cdx11 = CDX11Indexer(inputs=[warc_path], output=cdx_file, cdx11=True, post_append=True)
        cdx11.process_all()

        self.set_status('Processed.... Now reading....')

        with open(cdx_file) as f:
            for line in f:
                self.increment_counter('cdx', 'cdx_lines', 1)
                yield warc_uri, line.strip()

    def reducer(self, key, values):
        for value in values:
            yield key, value



class MRCDXIndexerStream(MRJob):

    #
    # Using this custom input format, rather than 'mapred.min.split.size' as
    # The default TextInputFormat auto-gunzips and leaves only the first record.
    #
    HADOOP_INPUT_FORMAT = 'uk.bl.wa.hadoop.mapred.UnsplittableInputFileFormat'

    LIBJARS = [ "../jars/warc-hadoop-recordreaders-3.2.0-SNAPSHOT-job.jar" ]

    SORT_VALUES = True

    JOBCONF = {
        'mapred.job.name': 'MRCDXIndexerStreamJob',
    } 

    def configure_options(self):
        super(CCJob, self).configure_options()
        # Pass through the runner so we can adapt:
        self.pass_through_option('--runner')
        self.pass_through_option('-r')

    def _wrap_protocols(self, step_num, step_type):
        '''Override the default implementation to directly parse stdin as a WARC.'''
        read_lines, write_line = super()._wrap_protocols(step_num, step_type)

        def read_records():
            from cdxj_indexer.main import CDX11Indexer

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

                    # Then index the file by reading stdin:
                    cdx_file = 'index.cdx'
                    cdx11 = CDX11Indexer(inputs='-', output=cdx_file, cdx11=True, post_append=True)
                    cdx11.process_all()

                    self.set_status('Processed.... Now reading....')

                    with open(cdx_file) as f:
                        for line in f:
                            self.increment_counter('cdx', 'cdx_lines', 1)
                            yield key, line.strip()
                else:
                    with open(path, 'rb') as stream:
                        pass

        # If this is a mapper, parse to records:
        if step_type == 'mapper':
            return read_records, write_line
        else:
            return read_lines, write_line

    def mapper(self, key, value):
        yield key, value

    def reducer(self, key, values):
        for value in values:
            yield key, value



if __name__ == '__main__':
    MRCDXIndexerCP.run()
    #MRCDXIndexerStream.run()

