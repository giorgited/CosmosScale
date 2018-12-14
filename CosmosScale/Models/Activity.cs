using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosScale.Models
{
    public class Activity
    {
        public Activity(string databaseName, string collectionName, DateTimeOffset activityTime, ActivityStrength activityStrength)
        {
            DatabaseName = databaseName;
            CollectionName = collectionName;
            ActivityTime = activityTime;
            ActivityStrength = activityStrength;
        }

        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }
        public DateTimeOffset ActivityTime { get; set; }
        public ActivityStrength ActivityStrength { get; set; }
        public string MetaDataType { get; } = "Activity";
    }
}
