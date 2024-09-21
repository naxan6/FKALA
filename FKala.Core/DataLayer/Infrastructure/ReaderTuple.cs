namespace FKala.Core
{

    public class ReaderTuple
    {
        public DateTime FileDate;
        public required string FilePath;
        public StreamReader? StreamReader;
        public bool MarkedAsSorted;
    }
}
