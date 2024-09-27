using FKala.Core.Interfaces;
using System.Text;
using static System.Net.Mime.MediaTypeNames;

namespace FKala.Core.DataLayer.Infrastructure
{
    public class BufferedWriter : IDisposable, IBufferedWriter
    {
        private readonly string _filePath;
        private FileStream _fileStream;
        private StreamWriter _streamWriter;

        private object LOCKI = new object();

        public object LOCK { get { return LOCKI; } }

        public bool disposed { get; private set; }

        public BufferedWriter(string filePath, int writeBuffer, bool append = true)
        {
            var fileMode = FileMode.Append;
            if (!append)
            {
                fileMode = FileMode.Create;
            }
            _filePath = filePath;
            _fileStream = new FileStream(filePath, fileMode, FileAccess.Write, FileShare.Read);
            _streamWriter = new StreamWriter(_fileStream, Encoding.UTF8, writeBuffer);
        }

        public void Append(string text)
        {            
            _streamWriter.Write(text);
        }
        public void Append(ReadOnlySpan<char> text)
        {            
            _streamWriter.Write(text);
        }

        public void AppendNewline()
        {
            _streamWriter.Write("\n");
        }

        public void Flush()
        {
            lock (LOCK)
            {
                if (!disposed)
                {
                    _streamWriter.Flush();
                }
            }
        }

        public void Dispose()
        {
            if (!disposed)
            {
                disposed = true;
                Flush();
                _streamWriter?.Dispose();
                _fileStream?.Dispose();
            }
        }

        public void Close()
        {
            _streamWriter.Close();
        }
    }
}
