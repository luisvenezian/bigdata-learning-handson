from starbase import Connection

c = Connection("127.0.0.1", "8001")

ratings = c.table('ratings')

if (ratings.exists()):
    print('Dropping existing ratings table\n')
    ratings.drop()
'''
# Creating a column family 'rating'
ratings.create('rating')

print('Parsing the ml-100k ratings data...\n')
ratingFile = open('C:\\Users\\luis.vanezian\\Downloads\\ml-100k\\u.data', 'r')

# Batch interface  
batch = ratings.batch()

for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID:rating}})

ratingFile.close()

print('Commiting ratings data to HBase via REST service\n')
batch.commit(finalize=True)

print('Getting back ratings for some users...\n')
print('Ratings for user ID: 1\n')
print(ratings.fetch('1'))
print('Ratings for user ID: 33\n')
print(ratings.fetch('33'))'''

