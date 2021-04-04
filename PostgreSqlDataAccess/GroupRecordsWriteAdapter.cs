using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PostgreSqlDataAccess
{
    public abstract class GroupRecordsWriteAdapter : IDisposable
    {
        readonly AGroupRecordsWriter[] Writers;
        readonly uint WritersQuantity;

        List<ParameterEvent> Events = new List<ParameterEvent>();
        readonly object EventsLock = new object();

        public delegate void StoredEventHandler (uint storedCount, List<Exception> errors);
        public event StoredEventHandler OnStored;

        //Task<uint> StoreTask = null;
        //readonly object WritesLock = new object();

        enum StoreThreadStates
        {
            Working,
            Terminating,
            Terminated
        }

        readonly Thread StoreThread;
        StoreThreadStates StoreThreadState;
        readonly object StoreThreadStateLock = new object();

        protected GroupRecordsWriteAdapter (string connectionString, uint writersQuantity, uint insertSize, uint transactionSize)
        {
            WritersQuantity = writersQuantity;
            Writers = new AGroupRecordsWriter[WritersQuantity];
            for (int i = 0; i < WritersQuantity; i++)
            {
                Writers[i] = createWriter( connectionString, insertSize, transactionSize );
            }

            StoreThreadState = StoreThreadStates.Working;
            StoreThread = new Thread ( new ThreadStart( storing ) );
            StoreThread.Start();
        }

        void storing ()
        {
            StoreThreadStates threadState;
            do {

                List<ParameterEvent> eventsToStore = null;
                lock (EventsLock) 
                {
                    if (Events.Count > 0) {
                        eventsToStore = Events;
                        Events = new List<ParameterEvent>();
                    }
                }
                if (eventsToStore != null)
                {
                    List<Exception> errors = new List<Exception>();
                    uint insertedCount = storeEventsTask( eventsToStore, errors );
                    OnStored?.Invoke( insertedCount, errors );
                }
                else {
                    Thread.Sleep( 50 );
                }

                lock (StoreThreadStateLock) {
                    threadState = StoreThreadState;
                }
            }
            while (threadState == StoreThreadStates.Working);

            lock (StoreThreadStateLock) {
                StoreThreadState = StoreThreadStates.Terminated;
            }
        }

        protected abstract AGroupRecordsWriter createWriter (string connectionString, uint insertSize, uint transactionSize);

        public uint GetQueueLength ()
        {
            lock (EventsLock) {
                return (uint)Events.Count;
            }
        }

        public void StoreEvents (List<ParameterEvent> eventsToStore)
        {
            lock (EventsLock) 
            {
                Events.AddRange( eventsToStore );
            }
        }

        //public uint StoreEvents (List<ParameterEvent> eventsToStore, List<Exception> errors)
        //{
        //    return storeEventsTask( eventsToStore, errors );
        //}

        //public void BeginStoreEvents (List<ParameterEvent> eventsToStore)
        //{
        //    lock (WritesLock)
        //    {
        //        StoreTask = Task<uint>.Run( () =>
        //        {
        //            List<Exception> errors = new List<Exception>();
        //            uint insertedCount = storeEventsTask( eventsToStore, errors );
        //            OnStored( insertedCount, errors );
        //            return insertedCount;
        //        } );
        //    }
        //}
        //public uint EndStoreEvents ()
        //{
        //    uint result = 0;
        //    if (StoreTask != null) 
        //    {
        //        StoreTask.Wait();
        //        result = StoreTask.Result;
        //        StoreTask = null;
        //    }
        //    return result;
        //}

        uint storeEventsTask (List<ParameterEvent> eventsToStore, List<Exception> errors)
        {
            uint totalQuantity = (uint)eventsToStore.Count;
            uint baseQuantityPerWriter = (totalQuantity - 1) / WritersQuantity;
            uint remainder = (totalQuantity - 1) - baseQuantityPerWriter * WritersQuantity;

            uint startIndex = 0;
            uint insertedCount = 0;

            List<Task<uint>> tasks = new List<Task<uint>>();
            try
            {
                for (int i = 0; i < WritersQuantity; i++)
                {
                    uint quantity = baseQuantityPerWriter;
                    if (i <= remainder)
                        quantity++;
                    if (quantity == 0)
                        break;

                    tasks.Add( runWriter( eventsToStore, Writers[i], startIndex, quantity ) );
                    startIndex += quantity;
                }

                foreach (Task<uint> task in tasks)
                {
                    task.Wait();
                    insertedCount += task.Result;
                }
            }
            catch (Exception error)
            {
                errors.Add( error );
                foreach (Task<uint> task in tasks)
                {
                    if (task.Exception != null)
                        errors.Add( task.Exception );
                }
            }
            return insertedCount;
        }

        Task<uint> runWriter (List<ParameterEvent> eventsToStore, AGroupRecordsWriter writer, uint startIndex, uint quantity)
        {
            IList<IGroupInsertableRecord> eventsToStoreA = eventsToStore.Cast<IGroupInsertableRecord>().ToList();

            Task <uint> task = Task.Run( () => {
                return writer.storeEvents( eventsToStoreA, startIndex, quantity );
            } );
            return task;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose (bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    lock (StoreThreadStateLock) {
                        StoreThreadState = StoreThreadStates.Terminating;
                    }
                    for (int i = 0; i < WritersQuantity; i++)
                    {
                        Writers[i]?.Dispose();
                    }

                    StoreThread.Join();

                    //if (!StoreThread.Join( 5000 )) {
                    //    StoreThread.Abort();
                    //}
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
