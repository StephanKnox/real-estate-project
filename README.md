I've created with project in order to get a good sense of housing market situation in cantons of Zug and Zurich and 
to identify potentially attractive offers for buying a property in Switzerland. The goal of the project is to create a data application which provides a detailed data-driven perspective for those looking to buy a property.
Currently the first version of the project is completed which already includes some insights on properties that are best value for money, location and proximity to public transportation. In next versions I plan to include other data points like municipal taxes, how far are locations of leasure (like ski resorts and hiking destinations), schools and kindergartens and possibly other factors as well as imploying machine learning capabilities to determines which trends and  correlations contibute to the real-estate price dynamics the most.
Currently I am only interested in properties that are for sale in these two cantons - analysis can be easily expanded to include properties that are for rent and in any number of locations in Switzerland.

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

# Object Storage and Data Lake
In order to not be locked to a specific cloud storage provider, MinIO object storage was chosen for this project.
[MinIO](https://min.io/) is s3 compatible object storage which serves as a gateway to the data which allows to stay cloud-agnostic and easily change to any of existing cloud providers if needed.
For storing and updating the real-estate data Delta Lake format was used. It is an open source storage framework built to implement a DataLakeHouse architecture. 
It has many useful features, most important ones for the project are: 
Support for schema evolution (schema of APIs responses is often changing, with columns being added and removed). This guarantees that ingestion pipeline will not break when schema is changed on API side
Upserts and ACID Transactions - merge, update and delete with ACID capabilities are performed directly on your distributed files



# Serving Layer

# Analytics

Tableau dashboard ...







