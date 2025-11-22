using System.Runtime.Serialization;

namespace Fkala.Core.Exceptions
{
    public class KalaErrorException : Exception
    {
        public KalaErrorException()
        {
        }

        public KalaErrorException(string? message) : base(message)
        {
        }

        public KalaErrorException(string? message, Exception? innerException) : base(message, innerException)
        {
        }

    }
}