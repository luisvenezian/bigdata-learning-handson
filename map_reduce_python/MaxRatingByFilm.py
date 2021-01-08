# Just another approach that looks like RatingsBreakdown.py
# to understand how to query with mapping and reducers functions
from mrjob.job import MRJob
from mrjob.step import MRStep

class MaxRatingByFilm(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, rating

    def reducer_count_ratings(self, key, values):
        yield key, max(values)

if __name__ == '__main__':
    MaxRatingByFilm.run()
