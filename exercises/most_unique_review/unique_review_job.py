"""
Find the review with the most unique words (i.e. words used in no other
reviews). Output the review's ID (see ``review_id`` below). In the case of a
tie, output both review IDs.

So if review A has the words "map map map reduce reduce" and review B has the
words "map combine combine reduce" then the output should be 'B' because it has
one unique word.

Hint: there will be three map/reduce steps.
"""
from mrjob.job import MRJob
from mrjob.protocol import (
    JSONValueProtocol,
    RawValueProtocol,
)


def review_id(obj):
    return "%s:%s:%s" % (obj['business_id'], obj['user_id'], obj['date'])


class MRUniqueReviewJob(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def steps(self):
        return [self.mr(mapper=self.mapper_review_words, reducer=self.reducer_sum_counts_per_review)]

    def mapper_review_words(self, _, obj):
        key = review_id(obj)
        if obj['type'] == 'review':
            for word in obj['text'].split():
                if word.strip():
                    yield key, (word, 1)

    def reducer_sum_counts_per_review(self, key, values):
        total = 0
        for word, value in values:
            total += value
        yield key, values[0][0], sum(values[0][1]))


if __name__ == '__main__':
    MRUniqueReviewJob.run()
