namespace BitcaskNet
{
    public class Block
    {
        public string FileId { get; set; }
        public int ValueSize { get; set; }
        public long ValuePos { get; set; }
        public long Timestamp { get; set; }
    }
}