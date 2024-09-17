using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Interfaces
{
    public interface IBufferedWriter : IDisposable
    {
        public object LOCK { get; }

        void Append(string text);
        void Append(ReadOnlySpan<char> text);
        void AppendNewline();
        void Flush();
    }
}
