
### Custom UDF to process Apache Log


#### LogLoad
The LogLoad class implements the userdefined Load function to load the Apache Log in the pig.

#### IpResolve

The IpResolve class resolve the IpAddress to the Country Name.It uses MaxMind API using distributed cache to hold the database for the ip lookup.
