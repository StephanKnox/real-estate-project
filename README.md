I created with project in order to get a good sense of housing market situation in cantons of Zug and Zurich and 
to identify potentially attractive offers for buying a property in Switzerland. Currently I am only interested in properties taht are for sale in these two cantons - analysis can be easily expanded to include properties that are for rent in any number of locations in Switzerland.

## Project Diagramm

<img width="788" alt="image" src="https://github.com/StephanKnox/real-estate-project/assets/123996543/6e78f78f-09ff-477a-8852-8bdc1e247536">

## Data Sources

Main data source for this project is one of the most popular swiss real-estate web portals which also offers an API interface allowing to make requests and receive a detailed property description. In order to make a call to it, it is necessary to provide a propery id (internal identifier of a property on the real-estate portal). There is also quite strict limit on the number of calls to the API, before receiving resource unavailable error. 

Considering points above two problems arise:

1) It is not possible to just get all properties in X km raduis from the location of interest in a single API request, those are made 1 by 1 for each property.
  
2) It is not feasible to make hundreds and thousands of requests a day, as already for only two locations - Zurich and Zug it will be hard to maintain the data up to date on a daily basis. If more locations are added later in order to expand the analysis it will be even harder.


## Getting the Data
Since requests to the API are made one by one, first property ids need to be identified. Besides the ids it would be useful to know property's price as well.

To get this data web scraping is used, that is, html pages are retrieved and then scraped for the data points of interest.
Python library called Selenium designed for web tests automation allows getting correctly rendered html pages. Those then are saved to a local storage. Saving them on disk rather than performing scraping "on the fly", in memory, increases both performance and fault tolerance, if process of requesting pages is interrupted or machine performing scraping loses power there is no need to restart the process from the beguinning - rather from the last succesfully retrieved property.

Web scraping itself is done with the help of another Python library - Beautiful Soup and a Python code which scrapes for properties ids and prices. 


## Custom Change Data Capture (CDC)
In order not to overburden an API with hundres of calls every day, some kind of mechanism was needed which will allow to determine which properties are of interest, i.e. properties that are new or existing properties that had a price change. 

This information should be available before making any API requests and only properties that are interesting should be requested. To solve this problem previously scraped property id and price are combined into one column - together these two consist a fingerprint: a unique identifier of each property and it's price. Knowing fingerprints of all properties in the location of interest on the real estate web portal allows to compare them with properties that are already ingested and decide which one request from the API. 

Instead of hunderes and potentially thousands of requests a day it is enough to make just a few dozens (new properties that are for sale are not added in big numbers even when considering the entirity of Switzerland). This mechanism allows to keep the data up to date, saving storage, bandwitdth, execution time and promotes a cortegeous use of an available API resource.

From technical perspective this change data capture mechanism is a left outer join between scraped properties dataframe with data in the delta table having fingerprint column as a join key. Join itself as well as sourcing the data is performed with Apache Spark. (PySpark)

# Storage

# Serving Layer

# Analytics

Tableau dashboard ...







