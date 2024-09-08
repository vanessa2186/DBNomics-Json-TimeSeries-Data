# DBNomics-Json-Data
Scrapping Json data from DBnomics open source website ("https://api.db.nomics.world/v22/series/CEPII/CHELEM-TRADE-CHEL?facets=1&format=json&limit=1000&observations=1").

The resulting files are:

- CHELEM_TRADE_CHEL.csv : This is the Time Series data. NOTE that this csv is not included due to its large size (44,974 KB).
- CHELEM_TRADE_CHEL_REFDATA.csv : This is the refernce data. It include deescriptions of the catagories' abbreviations.
- Dataset_Info.txt : This text file contains a description of the data. As with teh csv's above, this information was 
                     scrapped from the original json data.

