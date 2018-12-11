using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using CosmosScale.Models;

namespace CosmosScale.MetaDataOperator
{
    internal class TemporaryCollectionMetaDataOperator : IMetaDataOperator
    {
        public Task AddActiveCollection(string databaseName, string collectionName, int minimumRu)
        {
            throw new NotImplementedException();
        }

        public Task AddActivity(string databaseName, string collectionName, DateTimeOffset date, ActivityStrength activityStrength)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<ActiveCollection> GetAllActiveCollections()
        {
            throw new NotImplementedException();
        }

        public Activity GetLatestActivity(string databaseName, string collectionName)
        {
            throw new NotImplementedException();
        }
    }
}
