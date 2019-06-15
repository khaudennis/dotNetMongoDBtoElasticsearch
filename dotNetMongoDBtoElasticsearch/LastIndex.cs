using System;

namespace dotNetMongoDBtoElasticsearch
{
    class LastIndex
    {
        public LastIndex() { }
        public Guid Id { get; set; }
        public DateTime IndexDateTime { get; set; }
        public string IndexName { get; set; }
        public string LastIndexId { get; set; }
        public int LoadSuccessCount { get; set; }
        public int LoadFailCount { get; set; }
        public string LoadType { get; set; }
    }
}
