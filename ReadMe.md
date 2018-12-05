# Cosmos Auto Scaler

Cosmos Auto Scaler is a library for automatically scaling the RU based on the request load. The algorithm listens to the 429 returned from CosmosDB and increases the limit by increments of 500RU for each iteration. 
User is capable of adjusting the maximum RU to make sure that the software does not exceed the budget. The library is thread safe, there fore it can be called in parallel. Library will only do the scale operation once for every 1 second.
In addition, there is a cool down algorithm that tracks the activities for each collection and scales down based on the minimum RU provided by the user. Scale down occurs for 5 minute of inactivity.

## Getting Started

Using the library is simple. Search and download the latest nuget package.


### Usage

Create a cosmos scale operator for each collection that you are wanting to run operations on. In the following example, we set 600RU as minimum and 1500RU as maxmium. Database is called NewDatabase1, 
with collection name of "test". CosmosScaleOperator does require to pass down the DocumentCleint.


```
 DocumentClient _client = new DocumentClient(new Uri("COSMOS_URL"), "COSMOS_PASS",
              new ConnectionPolicy
              {
                  ConnectionMode = ConnectionMode.Direct,
                  ConnectionProtocol = Protocol.Tcp,
                  RequestTimeout = TimeSpan.FromMinutes(5) // this is important depending on the concurrency 
              });
```

```
CosmosScaleOperator op = new CosmosScaleOperator(600, 15000, "NewDatabase1", "test", _client);  
```

If you would like the library to create the Database and Collection for you, call the InitializeResourcesAsync function and pass in the desired request options
```
await op.InitializeResourcesAsync();  
```

To do CRUD operations, simply call the repsective function for 1-N documents.
```
await _cosmosOperator.QueryCosmos<OBJ>("SELECT * FROM C");
await _cosmosOperator.InsertDocumentAsync(some_document);
await _cosmosOperator.DeleteDocumentAsync(some_document.id);
await _cosmosOperator.ReplaceDocument(some_document.id, new_document);
```

## Authors

* **Giorgi (Gio) Tediashvili** - *Initial work* - [Gio](https://github.com/giorgited)

See also the list of [contributors](https://github.com/giorgited/CosmosScale/contributors) who participated in this project.


