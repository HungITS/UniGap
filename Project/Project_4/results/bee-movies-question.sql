USE beemovies;

/* Now that you have imported the data sets, let’s explore some of the tables. 
 To begin with, it is beneficial to know the shape of the tables and whether any column has null values.
 Further in this segment, you will take a look at 'movies' and 'genre' tables.*/



-- Segment 1:

-- Q1. Find the total number of rows in each table of the schema?
-- Type your code below:
SELECT 'movies' AS table_name, COUNT(*) AS total_rows FROM movie
UNION ALL
SELECT 'genre', COUNT(*) FROM genre
UNION ALL
SELECT 'ratings', COUNT(*) FROM ratings
UNION ALL
SELECT 'names', COUNT(*) FROM names;




-- Q2. Which columns in the movie table have null values?
-- Type your code below:
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'movie' 
AND is_nullable = 'YES';


-- Now as you can see four columns of the movie table has null values. Let's look at the at the movies released each year. 
-- Q3. Find the total number of movies released each year? How does the trend look month wise? (Output expected)

/* Output format for the first part:

+---------------+-------------------+
| Year			|	number_of_movies|
+-------------------+----------------
|	2017		|	2134			|
|	2018		|		.			|
|	2019		|		.			|
+---------------+-------------------+


Output format for the second part of the question:
+---------------+-------------------+
|	month_num	|	number_of_movies|
+---------------+----------------
|	1			|	 134			|
|	2			|	 231			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:
-- Year-wise movies count
SELECT year, COUNT(*) AS number_of_movies
FROM movie
GROUP BY year
ORDER BY year;

-- Month-wise movies count
SELECT MONTH(date_published) AS month_num, COUNT(*) AS number_of_movies
FROM movie
GROUP BY MONTH(date_published)
ORDER BY month_num;









/*The highest number of movies is produced in the month of March.
So, now that you have understood the month-wise trend of movies, let’s take a look at the other details in the movies table. 
We know USA and India produces huge number of movies each year. Lets find the number of movies produced by USA or India for the last year.*/
  
-- Q4. How many movies were produced in the USA or India in the year 2019??
-- Type your code below:
SELECT COUNT(*) AS number_of_movies
FROM movie
WHERE year = 2019 AND (country = 'USA' OR country = 'India');









/* USA and India produced more than a thousand movies(you know the exact number!) in the year 2019.
Exploring table Genre would be fun!! 
Let’s find out the different genres in the dataset.*/

-- Q5. Find the unique list of the genres present in the data set?
-- Type your code below:
SELECT DISTINCT genre FROM genre;



/* So, Bee Movies plans to make a movie of one of these genres.
Now, wouldn’t you want to know which genre had the highest number of movies produced in the last year?
Combining both the movie and genres table can give more interesting insights. */

-- Q6.Which genre had the highest number of movies produced overall?
-- Type your code below:
SELECT TOP 1 genre, COUNT(*) AS movie_count
FROM genre
GROUP BY genre
ORDER BY movie_count DESC;





/* So, based on the insight that you just drew, Bee Movies should focus on the ‘Drama’ genre. 
But wait, it is too early to decide. A movie can belong to two or more genres. 
So, let’s find out the count of movies that belong to only one genre.*/

-- Q7. How many movies belong to only one genre?
-- Type your code below:

SELECT COUNT(*) AS number_of_movies_with_one_genre
FROM (
    SELECT movie_id
    FROM genre
    GROUP BY movie_id
    HAVING COUNT(genre) = 1
) AS single_genre_movies;








/* There are more than three thousand movies which has only one genre associated with them.
So, this figure appears significant. 
Now, let's find out the possible duration of Bee Movies’ next project.*/

-- Q8.What is the average duration of movies in each genre? 
-- (Note: The same movie can belong to multiple genres.)


/* Output format:

+---------------+-------------------+
| genre			|	avg_duration	|
+-------------------+----------------
|	thriller	|		105			|
|	.			|		.			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:
SELECT genre, AVG(duration) AS avg_duration
FROM movie m
JOIN genre g ON m.id= g.movie_id
GROUP BY genre;








/* Now you know, movies of genre 'Drama' (produced highest in number in 2019) has the average duration of 106.77 mins.
Lets find where the movies of genre 'thriller' on the basis of number of movies.*/

-- Q9.What is the rank of the ‘thriller’ genre of movies among all the genres in terms of number of movies produced? 
-- (Hint: Use the Rank function)


/* Output format:
+---------------+-------------------+---------------------+
| genre			|		movie_count	|		genre_rank    |	
+---------------+-------------------+---------------------+
|drama			|	2312			|			2		  |
+---------------+-------------------+---------------------+*/
-- Type your code below:
SELECT TOP 1 genre, COUNT(*) AS movie_count,
       RANK() OVER (ORDER BY COUNT(*) DESC) AS genre_rank
FROM genre
GROUP BY genre
ORDER BY genre_rank;









/*Thriller movies is in top 3 among all genres in terms of number of movies
 In the previous segment, you analysed the movies and genres tables. 
 In this segment, you will analyse the ratings table as well.
To start with lets get the min and max values of different columns in the table*/




-- Segment 2:




-- Q10.  Find the minimum and maximum values in  each column of the ratings table except the movie_id column?
/* Output format:
+---------------+-------------------+---------------------+----------------------+-----------------+-----------------+
| min_avg_rating|	max_avg_rating	|	min_total_votes   |	max_total_votes 	 |min_median_rating|min_median_rating|
+---------------+-------------------+---------------------+----------------------+-----------------+-----------------+
|		0		|			5		|	       177		  |	   2000	    		 |		0	       |	8			 |
+---------------+-------------------+---------------------+----------------------+-----------------+-----------------+*/
-- Type your code below:
SELECT 
    MIN(avg_rating) AS min_avg_rating, MAX(avg_rating) AS max_avg_rating,
    MIN(total_votes) AS min_total_votes, MAX(total_votes) AS max_total_votes,
    MIN(median_rating) AS min_median_rating, MAX(median_rating) AS max_median_rating
FROM ratings;





    

/* So, the minimum and maximum values in each column of the ratings table are in the expected range. 
This implies there are no outliers in the table. 
Now, let’s find out the top 10 movies based on average rating.*/

-- Q11. Which are the top 10 movies based on average rating?
/* Output format:
+---------------+-------------------+---------------------+
| title			|		avg_rating	|		movie_rank    |
+---------------+-------------------+---------------------+
| Fan			|		9.6			|			5	  	  |
|	.			|		.			|			.		  |
|	.			|		.			|			.		  |
|	.			|		.			|			.		  |
+---------------+-------------------+---------------------+*/
-- Type your code below:
-- It's ok if RANK() or DENSE_RANK() is used too
SELECT TOP 10 m.title, r.avg_rating,
       RANK() OVER (ORDER BY avg_rating DESC) AS movie_rank
FROM movie m
JOIN ratings r on m.id = r.movie_id
ORDER BY movie_rank;







/* Do you find you favourite movie FAN in the top 10 movies with an average rating of 9.6? If not, please check your code again!!
So, now that you know the top 10 movies, do you think character actors and filler actors can be from these movies?
Summarising the ratings table based on the movie counts by median rating can give an excellent insight.*/

-- Q12. Summarise the ratings table based on the movie counts by median ratings.
/* Output format:

+---------------+-------------------+
| median_rating	|	movie_count		|
+-------------------+----------------
|	1			|		105			|
|	.			|		.			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:
-- Order by is good to have
SELECT median_rating, COUNT(*) AS movie_count
FROM ratings
GROUP BY median_rating
ORDER BY median_rating;









/* Movies with a median rating of 7 is highest in number. 
Now, let's find out the production house with which Bee Movies can partner for its next project.*/

-- Q13. Which production house has produced the most number of hit movies (average rating > 8)??
/* Output format:
+------------------+-------------------+---------------------+
|production_company|movie_count	       |	prod_company_rank|
+------------------+-------------------+---------------------+
| The Archers	   |		1		   |			1	  	 |
+------------------+-------------------+---------------------+*/
-- Type your code below:
SELECT TOP 1 production_company, COUNT(*) AS movie_count,
       RANK() OVER (ORDER BY COUNT(*) DESC) AS prod_company_rank
FROM movie m
JOIN ratings r ON m.id = r.movie_id
WHERE r.avg_rating > 8 AND m.production_company IS NOT NULL
GROUP BY production_company
ORDER BY prod_company_rank;








-- It's ok if RANK() or DENSE_RANK() is used too
-- Answer can be Dream Warrior Pictures or National Theatre Live or both

-- Q14. How many movies released in each genre during March 2017 in the USA had more than 1,000 votes?
/* Output format:

+---------------+-------------------+
| genre			|	movie_count		|
+-------------------+----------------
|	thriller	|		105			|
|	.			|		.			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:
SELECT genre, COUNT(*) AS movie_count
FROM genre g
JOIN movie m ON g.movie_id = m.id
JOIN ratings r ON m.id = r.movie_id
WHERE MONTH(m.date_published) = 3 AND m.year = 2017 AND country = 'USA' AND total_votes > 1000
GROUP BY genre;








-- Lets try to analyse with a unique problem statement.
-- Q15. Find movies of each genre that start with the word ‘The’ and which have an average rating > 8?
/* Output format:
+---------------+-------------------+---------------------+
| title			|		avg_rating	|		genre	      |
+---------------+-------------------+---------------------+
| Theeran		|		8.3			|		Thriller	  |
|	.			|		.			|			.		  |
|	.			|		.			|			.		  |
|	.			|		.			|			.		  |
+---------------+-------------------+---------------------+*/
-- Type your code below:
SELECT m.title, r.avg_rating, g.genre
FROM movie m
JOIN genre g ON m.id = g.movie_id
JOIN ratings r ON  g.movie_id = r.movie_id
WHERE m.title LIKE 'The%' AND avg_rating > 8;








-- You should also try your hand at median rating and check whether the ‘median rating’ column gives any significant insights.
-- Q16. Of the movies released between 1 April 2018 and 1 April 2019, how many were given a median rating of 8?
-- Type your code below:
SELECT COUNT(*) AS movie_count
FROM movie m
JOIN ratings r ON m.id = r.movie_id
WHERE m.date_published BETWEEN '2018-04-01' AND '2019-04-01'AND r.median_rating = 8;








-- Once again, try to solve the problem given below.
-- Q17. Do German movies get more votes than Italian movies? 
-- Hint: Here you have to find the total number of votes for both German and Italian movies.
-- Type your code below:
SELECT country, SUM(total_votes) AS total_votes
FROM movie m
JOIN ratings r ON m.id = r.movie_id
WHERE country IN ('Germany', 'Italy') 
GROUP BY country;







-- Answer is Yes

/* Now that you have analysed the movies, genres and ratings tables, let us now analyse another table, the names table. 
Let’s begin by searching for null values in the tables.*/




-- Segment 3:



-- Q18. Which columns in the names table have null values??
/*Hint: You can find null values for individual columns or follow below output format
+---------------+-------------------+---------------------+----------------------+
| name_nulls	|	height_nulls	|date_of_birth_nulls  |known_for_movies_nulls|
+---------------+-------------------+---------------------+----------------------+
|		0		|			123		|	       1234		  |	   12345	    	 |
+---------------+-------------------+---------------------+----------------------+*/
-- Type your code below:
SELECT
    (SELECT COUNT(*) FROM names WHERE name IS NULL) AS name_nulls,
    (SELECT COUNT(*) FROM names WHERE height IS NULL) AS height_nulls,
    (SELECT COUNT(*) FROM names WHERE date_of_birth IS NULL) AS date_of_birth_nulls,
    (SELECT COUNT(*) FROM names WHERE known_for_movies IS NULL) AS known_for_movies_nulls;









/* There are no Null value in the column 'name'.
The director is the most important person in a movie crew. 
Let’s find out the top three directors in the top three genres who can be hired by Bee Movies.*/

-- Q19. Who are the top three directors in the top three genres whose movies have an average rating > 8?
-- (Hint: The top three genres would have the most number of movies with an average rating > 8.)
/* Output format:

+---------------+-------------------+
| director_name	|	movie_count		|
+---------------+-------------------|
|James Mangold	|		4			|
|	.			|		.			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:
WITH top_genres AS (
    SELECT TOP 3 genre, COUNT(*) AS movie_count
    FROM genre
    JOIN ratings ON genre.movie_id = ratings.movie_id
    WHERE avg_rating > 8
    GROUP BY genre
    ORDER BY movie_count DESC
)
SELECT TOP 3 n.name, COUNT(*) AS movie_count
FROM movie m
JOIN genre g ON m.id = g.movie_id
JOIN ratings r ON m.id = r.movie_id
JOIN director_mapping d ON d.movie_id = m.id
JOIN names n ON n.id = d.name_id
WHERE genre IN (SELECT genre FROM top_genres) AND avg_rating > 8
GROUP BY n.name
ORDER BY movie_count DESC;








/* James Mangold can be hired as the director for Bee's next project. Do you remeber his movies, 'Logan' and 'The Wolverine'. 
Now, let’s find out the top two actors.*/

-- Q20. Who are the top two actors whose movies have a median rating >= 8?
/* Output format:

+---------------+-------------------+
| actor_name	|	movie_count		|
+-------------------+----------------
|Christain Bale	|		10			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:
SELECT TOP 2 n.name, COUNT(*) AS movie_count
FROM movie m
JOIN ratings r ON m.id = r.movie_id
JOIN role_mapping rm ON m.id = rm.movie_id
JOIN names n ON rm.name_id = n.id
WHERE median_rating >= 8 AND rm.category = 'actor'
GROUP BY n.name
ORDER BY movie_count DESC;







/* Have you find your favourite actor 'Mohanlal' in the list. If no, please check your code again. 
Bee Movies plans to partner with other global production houses. 
Let’s find out the top three production houses in the world.*/

-- Q21. Which are the top three production houses based on the number of votes received by their movies?
/* Output format:
+------------------+--------------------+---------------------+
|production_company|vote_count			|		prod_comp_rank|
+------------------+--------------------+---------------------+
| The Archers		|		830			|		1	  		  |
|	.				|		.			|			.		  |
|	.				|		.			|			.		  |
+-------------------+-------------------+---------------------+*/
-- Type your code below:
SELECT TOP 3 m.production_company, SUM(total_votes) AS vote_count,
       RANK() OVER (ORDER BY SUM(total_votes) DESC) AS prod_comp_rank
FROM movie m
JOIN ratings r ON m.id = r.movie_id
GROUP BY production_company
ORDER BY prod_comp_rank;





/*Yes Marvel Studios rules the movie world.
So, these are the top three production houses based on the number of votes received by the movies they have produced.

Since Bee Movies is based out of Mumbai, India also wants to woo its local audience. 
Bee Movies also wants to hire a few Indian actors for its upcoming project to give a regional feel. 
Let’s find who these actors could be.*/

-- Q22. Rank actors with movies released in India based on their average ratings. Which actor is at the top of the list?
-- Note: The actor should have acted in at least five Indian movies. 
-- (Hint: You should use the weighted average based on votes. If the ratings clash, then the total number of votes should act as the tie breaker.)

/* Output format:
+---------------+-------------------+---------------------+----------------------+-----------------+
| actor_name	|	total_votes		|	movie_count		  |	actor_avg_rating 	 |actor_rank	   |
+---------------+-------------------+---------------------+----------------------+-----------------+
|	Yogi Babu	|			3455	|	       11		  |	   8.42	    		 |		1	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
+---------------+-------------------+---------------------+----------------------+-----------------+*/
-- Type your code below:
SELECT n.name,
    SUM(r.total_votes) AS total_votes,
    COUNT(m.id) AS movie_count,
    SUM(r.avg_rating * r.total_votes) / SUM(r.total_votes) AS actor_avg_rating,
    RANK() OVER (ORDER BY SUM(r.avg_rating * r.total_votes) / SUM(r.total_votes) DESC, 
	SUM(r.total_votes) DESC) AS actor_rank
FROM movie m
JOIN ratings r ON m.id = r.movie_id
JOIN role_mapping rm ON m.id = rm.movie_id
JOIN names n ON n.id = rm.name_id
WHERE m.country = 'India'
GROUP BY n.name
HAVING COUNT(m.id) >= 5
ORDER BY actor_avg_rating DESC, total_votes DESC;





-- Top actor is Vijay Sethupathi

-- Q23.Find out the top five actresses in Hindi movies released in India based on their average ratings? 
-- Note: The actresses should have acted in at least three Indian movies. 
-- (Hint: You should use the weighted average based on votes. If the ratings clash, then the total number of votes should act as the tie breaker.)
/* Output format:
+---------------+-------------------+---------------------+----------------------+-----------------+
| actress_name	|	total_votes		|	movie_count		  |	actress_avg_rating 	 |actress_rank	   |
+---------------+-------------------+---------------------+----------------------+-----------------+
|	Tabu		|			3455	|	       11		  |	   8.42	    		 |		1	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
+---------------+-------------------+---------------------+----------------------+-----------------+*/
-- Type your code below:
SELECT TOP 5 n.name AS actress_name,
    SUM(r.total_votes) AS total_votes,
    COUNT(m.id) AS movie_count,
    ROUND((SUM(r.avg_rating * r.total_votes) / SUM(r.total_votes)),2) AS actress_avg_rating,
    RANK() OVER (ORDER BY SUM(r.avg_rating * r.total_votes) / SUM(r.total_votes) DESC, 
                 SUM(r.total_votes) DESC) AS actress_rank
FROM movie m
JOIN ratings r ON m.id = r.movie_id
JOIN role_mapping rm ON m.id = rm.movie_id
JOIN names n ON n.id = rm.name_id
WHERE m.country = 'India' AND m.languages = 'Hindi' AND rm.category = 'actress'
GROUP BY n.name
HAVING COUNT(m.id) >= 3
ORDER BY actress_avg_rating DESC, total_votes DESC;








/* Taapsee Pannu tops with average rating 7.74. 
Now let us divide all the thriller movies in the following categories and find out their numbers.*/


/* Q24. Select thriller movies as per avg rating and classify them in the following category: 

			Rating > 8: Superhit movies
			Rating between 7 and 8: Hit movies
			Rating between 5 and 7: One-time-watch movies
			Rating < 5: Flop movies
--------------------------------------------------------------------------------------------*/
-- Type your code below:
SELECT m.title,
       r.avg_rating,
       CASE
           WHEN r.avg_rating > 8 THEN 'Superhit movies'
           WHEN r.avg_rating BETWEEN 7 AND 8 THEN 'Hit movies'
           WHEN r.avg_rating BETWEEN 5 AND 7 THEN 'One-time-watch movies'
           ELSE 'Flop movies'
       END AS category
FROM movie m
JOIN ratings r ON m.id = r.movie_id
JOIN genre g ON g.movie_id = m.id
WHERE g.genre = 'Thriller'
ORDER BY r.avg_rating DESC;








/* Until now, you have analysed various tables of the data set. 
Now, you will perform some tasks that will give you a broader understanding of the data in this segment.*/

-- Segment 4:

-- Q25. What is the genre-wise running total and moving average of the average movie duration? 
-- (Note: You need to show the output table in the question.) 
/* Output format:
+---------------+-------------------+---------------------+----------------------+
| genre			|	avg_duration	|running_total_duration|moving_avg_duration  |
+---------------+-------------------+---------------------+----------------------+
|	comdy		|			145		|	       106.2	  |	   128.42	    	 |
|		.		|			.		|	       .		  |	   .	    		 |
|		.		|			.		|	       .		  |	   .	    		 |
|		.		|			.		|	       .		  |	   .	    		 |
+---------------+-------------------+---------------------+----------------------+*/
-- Type your code below:
SELECT g.genre AS genre,
    AVG(m.duration) AS avg_duration,
    SUM(AVG(m.duration)) OVER (PARTITION BY g.genre ORDER BY m.date_published ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_duration,
    AVG(AVG(m.duration)) OVER (PARTITION BY g.genre ORDER BY m.date_published ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_avg_duration
FROM movie m
JOIN genre g ON g.movie_id= m.id
GROUP BY g.genre, m.date_published
ORDER BY g.genre, m.date_published;




-- Round is good to have and not a must have; Same thing applies to sorting


-- Let us find top 5 movies of each year with top 3 genres.

-- Q26. Which are the five highest-grossing movies of each year that belong to the top three genres? 
-- (Note: The top 3 genres would have the most number of movies.)

/* Output format:
+---------------+-------------------+---------------------+----------------------+-----------------+
| genre			|	year			|	movie_name		  |worldwide_gross_income|movie_rank	   |
+---------------+-------------------+---------------------+----------------------+-----------------+
|	comedy		|			2017	|	       indian	  |	   $103244842	     |		1	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
+---------------+-------------------+---------------------+----------------------+-----------------+*/
-- Type your code below:
WITH TopGenres AS (
    SELECT TOP 3 g.genre AS genre,
           COUNT(m.id) AS movie_count
    FROM movie m
    JOIN genre g ON g.movie_id = m.id
    GROUP BY g.genre
    ORDER BY movie_count DESC
),
TopMovies AS (
    SELECT g.genre AS genre,
           m.date_published AS year,
           m.title AS movie_name,
           m.worlwide_gross_income,
           RANK() OVER (PARTITION BY g.genre, m.date_published ORDER BY m.worlwide_gross_income DESC) AS movie_rank
    FROM movie m
    JOIN genre g ON g.movie_id = m.id
    WHERE g.genre IN (SELECT genre FROM TopGenres)
)
SELECT genre,
       year,
       movie_name,
       worlwide_gross_income,
       movie_rank
FROM TopMovies
WHERE movie_rank <= 5
ORDER BY genre, year, movie_rank;

-- Top 3 Genres based on most number of movies




-- Finally, let’s find out the names of the top two production houses that have produced the highest number of hits among multilingual movies.
-- Q27.  Which are the top two production houses that have produced the highest number of hits (median rating >= 8) among multilingual movies?
/* Output format:
+-------------------+-------------------+---------------------+
|production_company |movie_count		|		prod_comp_rank|
+-------------------+-------------------+---------------------+
| The Archers		|		830			|		1	  		  |
|	.				|		.			|			.		  |
|	.				|		.			|			.		  |
+-------------------+-------------------+---------------------+*/
-- Type your code below:
WITH MultilingualMovies AS (
    SELECT m.id, m.production_company, m.title, r.avg_rating
    FROM movie m
    JOIN ratings r ON m.id = r.movie_id
    WHERE CHARINDEX(',', m.languages) > 0
),
HitMovies AS (
    SELECT production_company, COUNT(*) AS hit_count
    FROM MultilingualMovies
    WHERE avg_rating >= 8
    GROUP BY production_company
),
TopProductionHouses AS (
    SELECT production_company, hit_count,
           ROW_NUMBER() OVER (ORDER BY hit_count DESC) AS prod_comp_rank
    FROM HitMovies
)
SELECT production_company, hit_count AS movie_count, prod_comp_rank
FROM TopProductionHouses
WHERE prod_comp_rank <= 2
ORDER BY prod_comp_rank;







-- Multilingual is the important piece in the above question. It was created using POSITION(',' IN languages)>0 logic
-- If there is a comma, that means the movie is of more than one language


-- Q28. Who are the top 3 actresses based on number of Super Hit movies (average rating >8) in drama genre?
/* Output format:
+---------------+-------------------+---------------------+----------------------+-----------------+
| actress_name	|	total_votes		|	movie_count		  |actress_avg_rating	 |actress_rank	   |
+---------------+-------------------+---------------------+----------------------+-----------------+
|	Laura Dern	|			1016	|	       1		  |	   9.60			     |		1	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
+---------------+-------------------+---------------------+----------------------+-----------------+*/
-- Type your code below:
WITH SuperHitMovies AS (
    SELECT m.id, g.genre, m.title, r.avg_rating, r.total_votes, rm.name_id
    FROM movie m
    JOIN ratings r ON m.id = r.movie_id
    JOIN genre g ON g.movie_id = m.id
    JOIN role_mapping rm ON m.id = rm.movie_id
    WHERE g.genre = 'Drama' AND r.avg_rating > 8
),
ActressMovies AS (
    SELECT rm.name_id, COUNT(*) AS movie_count, SUM(r.total_votes) AS total_votes, 
           AVG(r.avg_rating) AS actress_avg_rating
    FROM SuperHitMovies r
    JOIN role_mapping rm ON r.id = rm.movie_id
    GROUP BY rm.name_id
),
TopActresses AS (
    SELECT n.name AS actress_name, total_votes, movie_count, actress_avg_rating,
           ROW_NUMBER() OVER (ORDER BY movie_count DESC, total_votes DESC) AS actress_rank
    FROM ActressMovies a
    JOIN names n ON n.id = a.name_id
)
SELECT actress_name, total_votes, movie_count, actress_avg_rating, actress_rank
FROM TopActresses
WHERE actress_rank <= 3
ORDER BY actress_rank;







/* Q29. Get the following details for top 9 directors (based on number of movies)
Director id
Name
Number of movies
Average inter movie duration in days
Average movie ratings
Total votes
Min rating
Max rating
total movie durations

Format:
+---------------+-------------------+---------------------+----------------------+--------------+--------------+------------+------------+----------------+
| director_id	|	director_name	|	number_of_movies  |	avg_inter_movie_days |	avg_rating	| total_votes  | min_rating	| max_rating | total_duration |
+---------------+-------------------+---------------------+----------------------+--------------+--------------+------------+------------+----------------+
|nm1777967		|	A.L. Vijay		|			5		  |	       177			 |	   5.65	    |	1754	   |	3.7		|	6.9		 |		613		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
+---------------+-------------------+---------------------+----------------------+--------------+--------------+------------+------------+----------------+

--------------------------------------------------------------------------------------------*/
-- Type you code below:
WITH DirectorMovies AS (
    SELECT m.id AS movie_id, d.name_id, m.duration, r.avg_rating, r.total_votes,
           DATEDIFF(DAY, LAG(m.date_published) OVER (PARTITION BY d.name_id ORDER BY m.date_published), m.date_published) AS inter_movie_duration
    FROM movie m
    JOIN ratings r ON m.id = r.movie_id
    JOIN director_mapping d ON d.movie_id = m.id
),
DirectorStats AS (
    SELECT name_id, 
           COUNT(movie_id) AS number_of_movies,
           AVG(inter_movie_duration) AS avg_inter_movie_days,
           AVG(avg_rating) AS avg_rating,
           SUM(total_votes) AS total_votes,
           MIN(avg_rating) AS min_rating,
           MAX(avg_rating) AS max_rating,
           SUM(duration) AS total_duration
    FROM DirectorMovies
    GROUP BY name_id
),
TopDirectors AS (
    SELECT TOP 9 n.id, n.name AS director_name, ds.*
    FROM DirectorStats ds
    JOIN names n ON n.id = ds.name_id
    ORDER BY ds.number_of_movies DESC
)
SELECT name_id, director_name, number_of_movies, avg_inter_movie_days, avg_rating,
       total_votes, min_rating, max_rating, total_duration
FROM TopDirectors;






