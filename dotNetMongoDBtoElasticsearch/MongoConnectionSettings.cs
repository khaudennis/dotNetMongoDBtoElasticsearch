namespace dotNetMongoDBtoElasticsearch
{
    class MongoConnectionSettings
    {
        public MongoConnectionSettings() { }
        public int limit { get; set; }
        public int skipCount { get; set; }
        public string connectionString { get; set; }
        public string databaseName { get; set; }
        public string collectionName { get; set; }
    }
}