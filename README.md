# Zurich and Zug Real-Estate Market Analytics

I've started this project in order to get a sense of housing market situation in cantons of Zug and Zurich and 
to identify potentially attractive offers for buying a property. 

# Data Sources

Main data source for this project is one of the most popular swiss real-estate web portals which also offers an API interface allowing to make requests and receive a very detailed description of a property. In order to make a call to an API it is necessary to provide a propery id (internal id of a property on the real-estate portal). There is also quite strict limit to the number of calls to the API, before receiving resource unavailable error. 

Considering points above two problems arise:

1) It is not possible to just get all properties in X km raduis from the location of interest in a single API request, those are made 1 by 1 for each property.
  
2) It is not feasible make hundreds abd thousands of requests a day, so already for only two locations - Zurich and Zug it will be hard to maintain the data up to date. If more locations are added later in order to expand the analysis it will be even harder.


# Getting the Data
Since requests to the API are made one by one, first property ids need to be identified. Besides the ids it would be useful to know property's price as well - this will permit to implement a custom CDC solution - details down below.

To get this data web scraping is used, that is, html pages are retrieved and then scraped for the data points of interest.
Python library called Selenium designed for web tests automation is used to get correctly rendered html pages. Those then are saved to a local storage. Saving them on disk rather than performing scraping "on the fly" increases both performance and fault 
tolerance, if process of requesting html pages is interrupted there is no need to restart it from the beguinning and rather from the last succesfully retrieved property.
Web scraping itself is done with the help of another Python library - Beautiful Soup and a Python method which scrapes for ids and prices. These two fields together consists a finger print, i.e. a unique combination which indicates if the property already exists or not in the data lakehouse table and if it exists if the price has changed.


# Custom Change Data Capture (CDC)
In order not to overburden an API with hundres of calls every day, there was a need in a mechanism which will allow to determine
which properties are of interest, i.e. properties that do not exists yet in the data lakehouse table or existing properties that had a price change. 
This information should be available before making any API requests and only properties that are interesting should be requested. To solve this problem web scraping is used. Web pages are first retrieved from real-estate portal and then scraped for two values: property id and property price. Together these two consist a fingerprint - a unique identifier of each property and it's price.






Tableau dashboard ...

<img width="788" alt="image" src="https://github.com/StephanKnox/real-estate-project/assets/123996543/6e78f78f-09ff-477a-8852-8bdc1e247536">





