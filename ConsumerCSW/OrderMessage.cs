namespace ConsumerCSW
{
    public class OrderMessage
    {
        public int Id { get; set; }
        public int MerchantId { get; set; }
        public string CountryOrigin { get; set; }
        public string CountryDestination { get; set; }
        public decimal Amount { get; set; }
        public int NumberOfItems { get; set; }
    }
}
