namespace FKala.Core
{

    public class ReaderTuple
    {
        public DateTime PathDate;
        public required string FilePath;
        public StreamReader? StreamReader;
        public bool MarkedAsSorted;
    }
}
