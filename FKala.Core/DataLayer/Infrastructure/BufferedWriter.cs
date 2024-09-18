﻿using FKala.Core.Interfaces;
using System.Text;

namespace FKala.Core.DataLayer.Infrastructure
{
    public class BufferedWriter : IDisposable, IBufferedWriter
    {
        private readonly string _filePath;
        private readonly StringBuilder _buffer = new StringBuilder();
        private readonly object _lock = new object();
        private FileStream _fileStream;
        private StreamWriter _streamWriter;

        private object LOCKI = new object();

        public object LOCK { get { return LOCKI; } }

        public BufferedWriter(string filePath)
        {
            _filePath = filePath;
            _fileStream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read);
            _streamWriter = new StreamWriter(_fileStream, Encoding.UTF8, 16384);
        }

        public void Append(string text)
        {
            _buffer.Append(text);
        }
        public void Append(ReadOnlySpan<char> text)
        {
            _buffer.Append(text);
        }

        public void AppendNewline()
        {
            _buffer.Append("\n");
        }

        public void Flush()
        {
            lock (LOCK)
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

        public void Close()
        {
            _streamWriter.Close();
        }
    }
}
