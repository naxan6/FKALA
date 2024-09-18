using FKala.Core.Interfaces;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.DataLayer.Infrastructure
{
    public class BufferedWriterService : IDisposable
    {
        private readonly ConcurrentDictionary<string, IBufferedWriter> _bufferedWriters = new ConcurrentDictionary<string, IBufferedWriter>();
        Task BufferedWriterFlushTask;

        public BufferedWriterService()
        {
            BufferedWriterFlushTask = Task.Run(() => FlushBuffersPeriodically());
        }

        private async Task FlushBuffersPeriodically()
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
                ForceFlushWriters();
            }
        }

        public void ForceFlushWriters()
        {
            foreach (var writer in _bufferedWriters)
            {
                lock (writer.Key)
                {
                    writer.Value.Dispose();
                    _bufferedWriters.Remove(writer.Key, out var removed);
                }
            }
        }

        public void Dispose()
        {
            foreach (var writer in _bufferedWriters.Values)
            {
                writer.Dispose();
            }
        }
        
        public void CreateWriteDispose(string filePath, Action<IBufferedWriter> writeAction)
        {
            lock (filePath)
            {                
                using var writer = new BufferedWriter(filePath);
                writeAction(writer);                
            }
        }

        public void DoWrite(string filePath, Action<IBufferedWriter> writeAction)
        {
            lock (filePath)
            {
                if (!_bufferedWriters.TryGetValue(filePath, out IBufferedWriter? writer))
                {
                    try
                    {
                        writer = new BufferedWriter(filePath);
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("Error at BufferedWriter with path <{filePath}>");
                        throw;
                    }
                    _bufferedWriters[filePath] = writer;
                }
                lock (writer!.LOCK)
                {
                    writeAction(writer);
                }
            }
        }
    }
}
