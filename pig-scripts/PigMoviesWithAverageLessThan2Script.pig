ratings = LOAD '/user/maria_dev/ml-100/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100/u.item' USING PigStorage('|')
        AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdblink: chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS countRating;

lessThanTwoStarMovies = FILTER avgRatings BY avgRating < 2.0;

lessThanTwoStarMoviesWithData  = JOIN lessThanTwoStarMovies BY movieID, nameLookup BY movieID;

mostBadRatedMoviesWithLessThanTwoStar = ORDER lessThanTwoStarMoviesWithData BY countRating DESC;

result = FOREACH mostBadRatedMoviesWithLessThanTwoStar GENERATE $0, $2, $4;

DUMP result;

describe result;
