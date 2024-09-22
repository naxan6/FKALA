
namespace FKala.Core.DataLayers
{
    [Serializable]
    internal class UnexpectedlyUnsortedException : Exception
    {
        public UnexpectedlyUnsortedException()
        {
        }

        public UnexpectedlyUnsortedException(string? message) : base(message)
        {
        }

        public UnexpectedlyUnsortedException(string? message, Exception? innerException) : base(message, innerException)
        {
        }
    }
}