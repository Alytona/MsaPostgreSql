using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PostgreSqlDataAccess
{
    public class WritingQueueLengthLogger : IDisposable
    {
        readonly EventsWriteAdapter EventsWriteAdapterInstance;

        enum ThreadStates
        {
            Working,
            Terminating,
            Terminated
        }
        readonly Thread LoggerThread;
        ThreadStates ThreadState;
        readonly object ThreadStateLock = new object();

        public delegate void LogQueueLenEventHandler (uint queueLen);
        public event LogQueueLenEventHandler OnLogged;

        public WritingQueueLengthLogger (EventsWriteAdapter eventsWriteAdapter)
        {
            EventsWriteAdapterInstance = eventsWriteAdapter;

            ThreadState = ThreadStates.Working;
            LoggerThread = new Thread( new ThreadStart( logging ) );
            LoggerThread.Start();
        }
        void logging ()
        {
            ThreadStates threadState;
            do
            {
                OnLogged?.Invoke( EventsWriteAdapterInstance.GetQueueLength() );
                Thread.Sleep( 100 );

                lock (ThreadStateLock)
                {
                    threadState = ThreadState;
                }
            }
            while (threadState == ThreadStates.Working);

            lock (ThreadStateLock)
            {
                ThreadState = ThreadStates.Terminated;
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose (bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    lock (ThreadStateLock)
                    {
                        ThreadState = ThreadStates.Terminating;
                    }
                    if (!LoggerThread.Join( 1000 ))
                    {
                        LoggerThread.Abort();
                    }
                }
                disposedValue = true;
            }
        }
        public void Dispose ()
        {
            Dispose( true );
        }
        #endregion
    }
}
