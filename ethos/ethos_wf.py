import re
import json
from collections import defaultdict
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


class MR_EThOS_WF(MRJob):
    '''
    Takes the EThOS data and replaces content with a word frequency map.
    '''

    # Only output the values (no key)
    OUTPUT_PROTOCOL = RawValueProtocol

    # A simple mapper that parses each document and reduces the content to word frequences
    def mapper(self, _, line):
        doc = json.loads(line)
        if 'content' in doc:
            d = defaultdict(int)
            for word in doc['content'].split():
                # Drop anything that is pure punctations:
                if re.match('^\W+$', word):
                    continue
                word = word.lower()
                d[word] += 1
            # Filter out low-frequency works:
            wf = {}
            for word in d.keys():
                if d[word] > 1:
                    wf[word] = d[word]
            # Store the word frequencies instead of the content.
            doc.pop('content')
            doc['word_freq'] = wf
        yield None, json.dumps(doc)

    def reducer(self, key, values):
        for value in values:
            yield None, value



if __name__ == '__main__':
    MR_EThOS_WF.run()
