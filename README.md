
### Custom UDF to process Apache Log


#### LogLoad
The LogLoad class implements the userdefined Load function to load the Apache Log in the pig.

#### IpResolve

The IpResolve class resolve the IpAddress to the Country Name.It uses MaxMind API using distributed cache to hold the database for the ip lookup.

#### Running the program

**Packaging**
```
mvn package
```

Register the jar and piggybank jar to the pig scripts

```
REGISTER '/path/to/customload-1.0-SNAPSHOT.jar'
REGISTER '/path/to/piggybank.jar'
```

To run the program:
```
pig -Dmapred.cache.files="/path/to/maxmindcountry_databasse" pig_script.pig
```


