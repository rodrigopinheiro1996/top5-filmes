from pyspark.sql.functions import *

file_path_movies = "/FileStore/tables/movies.csv"
file_path_reviews = "/FileStore/tables/reviews-1.csv"

movies_df = spark.read.csv(file_path_movies, header=True, inferSchema=True)
reviews_df = spark.read.csv(file_path_reviews, header=True, inferSchema=True)

df_join = movies_df.join(reviews_df, movies_df.filme_id == reviews_df.filme_id, 'inner')\
    .select(movies_df.título, movies_df.ano, movies_df.gênero, reviews_df.review, reviews_df.avaliação)\
    .distinct()

top_5 = df_join.orderBy(df_join.avaliação.desc()).limit(5)
display(top_5)