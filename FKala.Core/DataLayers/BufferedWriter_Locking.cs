using FKala.Core.Interfaces;
using System.Text;

namespace FKala.Core
{
    public class BufferedWriter_Locking : IDisposable, IBufferedWriter
    {
        private readonly string _filePath;
        private readonly StringBuilder _buffer = new StringBuilder();
        private readonly object _lock = new object();
        private FileStream _fileStream;
        private StreamWriter _streamWriter;

        public BufferedWriter_Locking(string filePath)
        {
            _filePath = filePath;
            _fileStream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read);
            _streamWriter = new StreamWriter(_fileStream, Encoding.UTF8);
        }


        public void Append(string text)
        {
            lock (_lock)
            {
                _buffer.Append(text);
            }
        }

        public void Append(ReadOnlySpan<char> text)
        {
            lock (_lock)
            {
                _buffer.Append(text);
            }
        }

        public void AppendNewline()
        {

            lock (_lock)
            {
                _buffer.Append("\n");
            }

        }

        public void Flush()
        {

            lock (_lock)
            {
                if (_buffer.Length == 0) return;

                _streamWriter.Write(_buffer.ToString());
                _buffer.Clear();
                _streamWriter.Flush();
            }

        }

        public void Dispose()
        {
            Flush();
            _streamWriter?.Dispose();
            _fileStream?.Dispose();
        }

       
    }
}
