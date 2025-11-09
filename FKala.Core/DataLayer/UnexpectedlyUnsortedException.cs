
namespace FKala.Core.DataLayers
{
    [Serializable]
    public class UnexpectedlyUnsortedException : Exception
    {
        public string File { get; set; }

        public UnexpectedlyUnsortedException(string? message, string file) : base(message)
        {
            File = file;
        }

        public UnexpectedlyUnsortedException(string? message, string file, Exception? innerException) : base(message, innerException)
        {
            File = file;
        }
    }
}