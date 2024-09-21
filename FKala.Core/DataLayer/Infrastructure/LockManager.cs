using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.DataLayer.Infrastructure
{

    public class LockManager
    {
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new ConcurrentDictionary<string, SemaphoreSlim>();

        /// <summary>
        /// Erzeugt oder erhält einen Lock für den angegebenen Schlüssel.
        /// </summary>
        /// <param name="key">Der Schlüssel, für den der Lock erworben werden soll.</param>
        /// <returns>Ein IDisposable, das den Lock freigibt, wenn es entsorgt wird.</returns>
        public IDisposable AcquireLock(string key)
        {
            // Erstellen eines SemaphoreSlim für den Schlüssel, wenn noch nicht vorhanden
            var semaphore = _locks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));

            // Warte auf den Lock
            semaphore.Wait();

            return new Releaser(() => ReleaseLock(key));
        }

        public bool IsLocked(string key)
        {
            // Erstellen eines SemaphoreSlim für den Schlüssel, wenn noch nicht vorhanden
            var semaphore = _locks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));

            // Warte auf den Lock
            return semaphore.CurrentCount == 0;
        }

        /// <summary>
        /// Gibt den Lock für den angegebenen Schlüssel frei.
        /// </summary>
        /// <param name="key">Der Schlüssel, für den der Lock freigegeben werden soll.</param>
        private void ReleaseLock(string key)
        {
            if (_locks.TryGetValue(key, out var semaphore))
            {
                semaphore.Release();
            }
        }

        private class Releaser : IDisposable
        {
            private readonly Action _releaseAction;
            private bool _disposed;

            public Releaser(Action releaseAction)
            {
                _releaseAction = releaseAction;
            }

            public void Dispose()
            {
                if (!_disposed)
                {
                    _releaseAction();
                    _disposed = true;
                }
            }
        }
    }

}
