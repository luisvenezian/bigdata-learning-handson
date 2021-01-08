# This code runs mappers and reducers to transform and agregrate the 
# just by using Python.
# How to run? 
# First, get the data: http://media.sundog-soft.com/data/u.data
# Then, in CLI interface execute: python RatingsBreakdown.py u.data
# Expected output:
# "1"     6111
# "2"     11370
# "3"     27145
# "4"     34174
# "5"     21203

from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer_count_ratings(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    RatingsBreakdown.run()