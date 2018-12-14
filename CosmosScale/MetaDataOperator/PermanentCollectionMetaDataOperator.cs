using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CosmosScale.Models;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace CosmosScale.MetaDataOperator
{
    internal class PermanentCollectionMetaDataOperator : IMetaDataOperator
    {
        private const string _metaDataCollectionName = "_autoscale-metadata";
        private Uri _metaDataCollectionUri;
        private DocumentClient _documentClient;

        public PermanentCollectionMetaDataOperator(DocumentClient client, string databaseName)
        {
            _documentClient = client;

            Database database = new Database();
            database.Id = databaseName;
            var databaseUri = UriFactory.CreateDatabaseUri(databaseName);

            DocumentCollection documentCollection = new DocumentCollection();
            documentCollection.Id = _metaDataCollectionName;

            _documentClient.CreateDocumentCollectionIfNotExistsAsync(databaseUri, documentCollection).Wait();

            _metaDataCollectionUri = UriFactory.CreateDocumentCollectionUri(databaseName, _metaDataCollectionName);
        }

        public async Task AddActiveCollection(string databaseName, string collectionName, int minimumRu)
        {
            await _documentClient.CreateDocumentAsync(_metaDataCollectionUri, new ActiveCollection(databaseName, collectionName, minimumRu));
        }
        public IEnumerable<ActiveCollection> GetAllActiveCollections()
        {
            return _documentClient.CreateDocumentQuery<ActiveCollection>(_metaDataCollectionUri,
                       new SqlQuerySpec()
                       {
                           QueryText = "select * from c where c.MetaDataType = 'ActiveCollection'"
                       }).AsEnumerable();
        }



        public async Task AddActivity(string databaseName, string collectionName, DateTimeOffset date, ActivityStrength activityStrength)
        {
            await _documentClient.CreateDocumentAsync(_metaDataCollectionUri, new OperationActivity(databaseName, collectionName, date, activityStrength));            
        }

        public OperationActivity GetLatestActivity(string databaseName, string collectionName)
        {
            var items = _documentClient.CreateDocumentQuery<OperationActivity>(_metaDataCollectionUri,
                    new SqlQuerySpec()
                    {
                        QueryText = "select top 1 * from c where c.MetaDataType = 'OperationActivity' order by c.ActivityTime desc"
                    }).ToList();

            return items.FirstOrDefault();
        }
        

        public DateTimeOffset GetLatestScaleUp(string databaseName, string collectionName)
        {
            var items = _documentClient.CreateDocumentQuery<ScaleActivity>(_metaDataCollectionUri,
                      new SqlQuerySpec()
                      {
                          QueryText = "select top 1 * from c where c.MetaDataType = 'ScaleActivity' order by c.ActivityTime desc"
                      }).ToList();

            return items.FirstOrDefault().ScaleTime;
        }

        public async Task AddScaleActivity(string databaseName, string collectionName, int ru, DateTimeOffset datetime)
        {
            await _documentClient.CreateDocumentAsync(_metaDataCollectionUri, new ScaleActivity(databaseName, collectionName, ru, datetime));
        }
    }
}
