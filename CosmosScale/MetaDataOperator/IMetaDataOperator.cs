using CosmosScale.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CosmosScale.MetaDataOperator
{
    internal interface IMetaDataOperator
    {
        Task AddActivity(string databaseName, string collectionName, DateTimeOffset date, ActivityStrength activityStrength);
        OperationActivity GetLatestActivity(string databaseName, string collectionName);


        Task AddActiveCollection(string databaseName, string collectionName, int minimumRu);
        IEnumerable<ActiveCollection> GetAllActiveCollections();

        DateTimeOffset GetLatestScaleUp(string databaseName, string collectionName);
        Task AddScaleActivity(string databaseName, string collectionName, int ru, DateTimeOffset datetime);
    }
}
