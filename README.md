# Zurich and Zug Real-Estate Market Analytics

I've started this project in order to get a sense of housing market situation in cantons of Zug and Zurich and 
to identify potentially attractive offers for buying a property. 

Main data source is one of the most popular swiss real-estate web portals which also offers an API
interface that allows one to make requests and receive a very detailed description of a property.


Pipeline takes the input file splitted in 5 parts and makes a call to OMDB API for each title 
creating enriched files and uploads them over SSH to SFTP server on AWS linked to S3 "raw" bucket.
From "raw" storage new files are transfered into the staging bucket and from there data is loaded 
into Postgres database instance which functions as a serving layer for reporting via Tableau.

Tableau dashboard shows best movies per decade, genre, best years in cinematography quantified by me as when 
number of good movies produced in a year is 1.5 times higher than the average per year from 1920s up to 
present day. It also shows how my preferences compare to IMDB rankings and the rating of movies I'd like to watch in the future.

<img width="788" alt="image" src="https://github.com/StephanKnox/real-estate-project/assets/123996543/6e78f78f-09ff-477a-8852-8bdc1e247536">





