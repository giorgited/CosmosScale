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
        private const string _metaDataCollectionName = "___autoscale-metadata";
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

        public async Task AddActiveCollections(string databaseName, string collectionName, int minimumRu)
        {
            var activeColl = _documentClient.CreateDocumentQuery<ActiveCollection>(_metaDataCollectionUri,
                    new SqlQuerySpec()
                    {
                        QueryText = string.Format("select top 1 * from c where c.DatabaseName = '{0}' and c.CollectionName = '{1}' and c.MetaDataType = 'ActiveCollection'", databaseName, collectionName)
                    }).FirstOrDefault();

            if (activeColl == null)
            {
                await _documentClient.CreateDocumentAsync(_metaDataCollectionUri, new ActiveCollection(databaseName, collectionName, minimumRu));
            }
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
            await _documentClient.CreateDocumentAsync(_metaDataCollectionUri, new Activity(databaseName, collectionName, date, activityStrength));            
        }

        public Activity GetLatestActivity(string databaseName, string collectionName)
        {
            return _documentClient.CreateDocumentQuery<Activity>(_metaDataCollectionUri,
                    new SqlQuerySpec()
                    {
                        QueryText = "select top1 * from c where c.MetaDataType = 'ActiveCollection order by c.ActivityTime desc'"
                    }).FirstOrDefault();
        }

        public Task AddActiveCollection(string databaseName, string collectionName, int minimumRu)
        {
            throw new NotImplementedException();
        }
        
    }
}
