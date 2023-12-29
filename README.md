# Zurich and Zug Real-Estate Market Analytics

I've started this project in order to get a sense of housing market situation in cantons of Zug and Zurich and 
to identify potentially attractive offers for buying a property. 

Main data source for the project is one of the most popular swiss real-estate web portals which also offers an API
interface allowing to make requests and receive a very detailed description of a property.
In order to make a call to an API it is necessary to provide a propery id (internal id of a property on the real-estate web portal). There is also quite strict limit to the number of calls to the API, before receiving resource unavailable
error. 
In order to get the propery id web scraping is needed, that is, get correctly rendered .html page and then scrape it for the 
id. Many web sites including this one make it hard to just get .html page contents.
Luckly Python library called Selenium designed for web tests automation can help to circumvent that obstacle.
Web scraping itself is done with the help of another Python library - Beautiful Soup.
In order to increase web scraping performance all .html pages first retrieved and saved to a local storage
and after scraped for property id and price. These two fields consists a finger print, i.e. a unique combination which
indicates if the property already exists or not in the data lakehouse table and if it exists if the price has changed.






Pipeline takes the input file splitted in 5 parts and makes a call to OMDB API for each title 
creating enriched files and uploads them over SSH to SFTP server on AWS linked to S3 "raw" bucket.
From "raw" storage new files are transfered into the staging bucket and from there data is loaded 
into Postgres database instance which functions as a serving layer for reporting via Tableau.

Tableau dashboard shows best movies per decade, genre, best years in cinematography quantified by me as when 
number of good movies produced in a year is 1.5 times higher than the average per year from 1920s up to 
present day. It also shows how my preferences compare to IMDB rankings and the rating of movies I'd like to watch in the future.

<img width="788" alt="image" src="https://github.com/StephanKnox/real-estate-project/assets/123996543/6e78f78f-09ff-477a-8852-8bdc1e247536">





