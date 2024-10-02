namespace FKala.Core
{

    public class ReaderTuple
    {
        public DateOnly FileDate;
        public required string FilePath;
        public StreamReader? StreamReader;
        public bool MarkedAsSorted;
    }
}
