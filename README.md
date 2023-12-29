# Zurich and Zug Real-Estate Market Analytics

I've started this project in order to get a sense of housing market situation in cantons of Zug and Zurich and 
to identify potentially attractive offers for buying a property. 

# Getting the Data

Main data source for this project is one of the most popular swiss real-estate web portals which also offers an API
interface allowing to make requests and receive a very detailed description of a property.
In order to make a call to an API it is necessary to provide a propery id (internal id of a property on the real-estate portal). There is also quite strict limit to the number of calls to the API, before receiving resource unavailable
error. 

In order to get the propery id web scraping is needed, that is, get correctly rendered .html page and then scrape it for the 
id. Many web sites including this one make it hard to just get .html page contents.
Luckly Python library called Selenium designed for web tests automation can help to circumvent that obstacle.
Web scraping itself is done with the help of another Python library - Beautiful Soup.
In order to increase web scraping performance all .html pages first retrieved and saved to a local storage
and after scraped for property id and price. These two fields consists a finger print, i.e. a unique combination which
indicates if the property already exists or not in the data lakehouse table and if it exists if the price has changed.

# Custom Change Data Capture (CDC)
In order not to overburden an API with hundres of calls every day, there was a need in a mechanism which will allow to determine
which properties are of interest, i.e. properties that do not exists yet in the data lakehouse table or existing properties that had a price change. 
This information should be available before making any API requests and only properties that are interesting should be requested. To solve this problem web scraping is used. Web pages are first retrieved from real-estate portal and then scraped for two values: property id and property price. Together these two consist a fingerprint - a unique identifier of each property and it's price.






Tableau dashboard shows best movies per decade, genre, best years in cinematography quantified by me as when 
number of good movies produced in a year is 1.5 times higher than the average per year from 1920s up to 
present day. It also shows how my preferences compare to IMDB rankings and the rating of movies I'd like to watch in the future.

<img width="788" alt="image" src="https://github.com/StephanKnox/real-estate-project/assets/123996543/6e78f78f-09ff-477a-8852-8bdc1e247536">





