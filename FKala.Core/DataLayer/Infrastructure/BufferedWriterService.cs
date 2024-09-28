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
        private readonly int WriteBuffer;

        public IDataLayer DataLayer { get; }

        Task BufferedWriterFlushTask;

        LockManager lockManager = new LockManager();

        public BufferedWriterService(int writeBuffer, IDataLayer dataLayer)
        {
            this.WriteBuffer = writeBuffer;
            this.DataLayer = dataLayer;
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
                ForceFlushWriter(writer.Key);
        }

        public void ForceFlushWriter(string filepath)
        {
            using (var lockHandle = lockManager.AcquireLock(filepath))
            {
                if (!_bufferedWriters.ContainsKey(filepath))
                {
                    return;
                }

                _bufferedWriters.Remove(filepath, out var removed);
                removed!.Dispose();
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
                using (var writer = new BufferedWriter(filePath, WriteBuffer, append))
                {
                    writeAction(writer);

                    writer.Flush();
                    writer.Close();
                }
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
                        writer = new BufferedWriter(filePath, WriteBuffer);
                    }
                    catch (Exception)
                    {
                        var measurement = new DirectoryInfo(filePath).Parent!.Parent!.Parent!.Name;
                        if (DataLayer.IsBlacklisted(measurement, true))
                        {
                            return;
                        }
                        var dir = Path.GetDirectoryName(filePath);
                        if (!Directory.Exists(dir))
                        {
                            Directory.CreateDirectory(dir!);
                        }
                        try
                        {
                            writer = new BufferedWriter(filePath, WriteBuffer);
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
