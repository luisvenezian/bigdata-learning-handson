# This way we can then add up all the ratings for each movie, and
# the total number of ratings for each movie (which lets us compute the average)
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0)) # (Movie ID, (RatingStars, 1)

if __name__ == "__main__":
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # Load up our movie ID -> movie name lookup table
    movieNames = loadMovieNames()

    # Load up the raw u.data file
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100/u.data")

    # Convert to (movieID, (rating, 1.0))
    movieRatings = lines.map(parseInput)

    # Reduce to (movieID, (sumOfRatings, totalRatings))
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda prev_val, next_val: ( prev_val[0] + next_val[0], prev_val[1] + next_val[1] ) )

    ratingTotalsAndCountGreaterThanTen = ratingTotalsAndCount.filter(lambda x: True if x[1][1] > 10 else False) # x[1][1] is TotalRatings

    # Map to (rating, averageRating)
    averageRatings = ratingTotalsAndCountGreaterThanTen.mapValues(lambda row : row[0] / row[1]) # where position 0 is SumOfrating column and 1 is totalCount

    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the top 10 results
    results = sortedMovies.take(10)

    # Print them out:
    for result in results:
        print(movieNames[result[0]], result[1])
