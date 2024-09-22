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

        LockManager lockManager = new LockManager();

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
                using (var lockHandle = lockManager.AcquireLock(writer.Key))
                {
                    lock (writer.Value.LOCK)
                    {
                        _bufferedWriters.Remove(writer.Key, out var removed);
                        removed!.Dispose();
                    }
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

        public void CreateWriteDispose(string filePath, bool append, Action<IBufferedWriter> writeAction)
        {
            using (lockManager.AcquireLock(filePath))
            {
                using var writer = new BufferedWriter(filePath, append);
                writeAction(writer);
            }
        }

        public void DoWrite(string filePath, Action<IBufferedWriter> writeAction)
        {
            using (lockManager.AcquireLock(filePath))
            {
                if (!_bufferedWriters.TryGetValue(filePath, out IBufferedWriter? writer))
                {
                    try
                    {
                        writer = new BufferedWriter(filePath);
                    }
                    catch (Exception)
                    {
                        var dir = Path.GetDirectoryName(filePath);
                        if (!Directory.Exists(dir))
                        {
                            Directory.CreateDirectory(dir);
                        }
                        try
                        {
                            writer = new BufferedWriter(filePath);
                        }
                        catch (Exception)
                        {
                            Console.WriteLine("Error at BufferedWriter with path <{filePath}>");
                            throw;
                        }
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
