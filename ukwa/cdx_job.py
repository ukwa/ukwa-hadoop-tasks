from mrjob.job import MRJob

class MRCDXIndexer(MRJob):

    def mapper_raw(self, warc_path, warc_uri):
        from cdxj_indexer.main import CDX11Indexer

        cdx_file = 'index.cdx'

        cdx11 = CDX11Indexer(inputs=[warc_path], output=cdx_file, cdx11=True, post_append=True)
        cdx11.process_all()

        print('Processed.... Now reading....')

        with open(cdx_file) as f:
            for line in f:
                self.increment_counter('cdx', 'cdx_lines', 1)
                yield warc_uri, line.strip()

    def reducer(self, key, values):
        for value in values:
            yield key, value



if __name__ == '__main__':
    MRCDXIndexer.run()

