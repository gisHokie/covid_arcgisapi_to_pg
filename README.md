# ETL from ArcGIS COVID19 API to a Postgres Database

NOTES:
1) No Scheduler is used yet.
2) API from ESRI source.
3) To provide a dynamic covid map layer so that new data is display, the Python ETL script must run at a scheudled interval (i.e. daily, hourly, twice a day...etc).
4) A Postgres feature class/table was created specifically for the ESRI Covide source for this demo.  This can be changed by providing a generic table and customizing the ETL script to conform any dataset to that table.
