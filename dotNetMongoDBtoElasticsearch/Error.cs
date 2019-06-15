namespace dotNetMongoDBtoElasticsearch
{
    class Error
    {
        public Error() { }
        public string indexName { get; set; }
        public string documentId { get; set; }
        public string documentType { get; set; }
        public string code { get; set; }
        public string reason { get; set; }
    }
}