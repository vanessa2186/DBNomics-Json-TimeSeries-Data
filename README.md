# DBNomics-Json-Data
Scrapping Json data from DBnomics open source website ("https://api.db.nomics.world/v22/series/CEPII/CHELEM-TRADE-CHEL?facets=1&format=json&limit=1000&observations=1").

The resulting files are:

- CHELEM_TRADE_CHEL.csv : This is the Time Series data. NOTE that this csv file is not included in thsi repo due to its
                          large size (~44,974 KB).
- CHELEM_TRADE_CHEL_REFDATA.csv : This is the reference data. It include descriptions of category abbreviations.
- Dataset_Info.txt : This text file contains a brief description of the data. As with the csv's above, this information was 
                     scrapped from the json data (same link as above).



FURTHER DEVELOPMENTS:
- A recusrsive approach would have been mre suitable here. I'm working on a new version that will use thsi approach.
- The lines 157 onwards can be wrapped up in a function to process each step, using a for-loop iterating through each
  each url (there is actually 3 of them and the one below is just one of them).
