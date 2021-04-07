using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PostgreSqlDataAccess
{
    /// <summary>
    /// Потокобезопасный счетчик остатка для буфера записи
    /// </summary>
    class ThreadSafeCounter
    {
        readonly object ValueLock = new object();

        public uint _value;

        public uint Value
        {
            get {
                lock (ValueLock) { return _value; }
            }
            set {
                lock (ValueLock) { _value = value; }
            }
        }
        public void subtract (uint quantity)
        {
            lock (ValueLock) { 
                _value -= quantity; 
            }
        }
        public void add (uint quantity)
        {
            lock (ValueLock)
            {
                _value += quantity;
            }
        }
    }

    /// <summary>
    /// Потокобезопасный буфер, в котором события накапливаются перед записью
    /// </summary>
    class EventsPrepareBuffer
    {
        List<ParameterEvent> _events = new List<ParameterEvent>();
        readonly object _eventsLock = new object();

        public uint Length
        {
            get
            {
                lock (_eventsLock)
                {
                    return (uint)_events.Count;
                }
            }
        }
        public List<ParameterEvent> replaceBufferIfNotEmpty ()
        {
            lock (_eventsLock)
            {
                if (_events.Count == 0)
                    return null;

                List<ParameterEvent> eventsToStore = _events;
                _events = new List<ParameterEvent>();
                return eventsToStore;
            }
        }
        public void addEvents (List<ParameterEvent> eventsToStore)
        {
            lock (_eventsLock)
            {
                _events.AddRange( eventsToStore );
            }
        }
    }

    class StoreThread
    {
        enum StoreThreadStates
        {
            Working,
            Terminating,
            Terminated
        }
        StoreThreadStates _storeThreadState;

        readonly Thread _storeThread;
        readonly object _storeThreadStateLock = new object();

        readonly ThreadStart _storeLogic;

        public StoreThread (ThreadStart storeLogic)
        {
            _storeLogic = storeLogic;

            _storeThreadState = StoreThreadStates.Working;
            _storeThread = new Thread( threadMethod );
            _storeThread.Priority = ThreadPriority.BelowNormal;
        }

        public void start ()
        {
            _storeThread.Start();
        }
        public void terminate ()
        {
            lock (_storeThreadStateLock)
            {
                _storeThreadState = StoreThreadStates.Terminating;
            }
        }
        public void waitForTermination ()
        {
            _storeThread.Join();

            //if (!StoreThread.Join( 5000 )) {
            //    StoreThread.Abort();
            //}
        }

        void threadMethod ()
        {
            StoreThreadStates threadState;
            do
            {
                _storeLogic.Invoke();

                lock (_storeThreadStateLock)
                {
                    threadState = _storeThreadState;
                }
            }
            while (threadState == StoreThreadStates.Working);
            lock (_storeThreadStateLock)
            {
                _storeThreadState = StoreThreadStates.Terminated;
            }
        }
    }

    public abstract class GroupRecordsWriteAdapter : IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        readonly AGroupRecordsWriter[] Writers;
        readonly uint WritersQuantity;

        /// <summary>
        /// Обработчик окончания добавления внутреннего буфера в БД
        /// </summary>
        /// <param name="storedCount">Количество добавленных в БД записей</param>
        /// <param name="errors">Список ошибок, возникших при добавлении</param>
        public delegate void StoredEventHandler (uint storedCount, List<Exception> errors);

        /// <summary>
        /// Событие, которое вызывается после записи всего внутреннего буфера для передачи количества записей, 
        /// добавленных в БД и списка ошибок.
        /// </summary>
        public event StoredEventHandler OnStored;

        bool _storing;
        readonly object StoringLock = new object();
        bool Storing
        {
            get {
                lock (StoringLock) { return _storing; }
            }
            set {
                lock (StoringLock) { _storing = value; }
            }
        }

        readonly StoreThread _storingThread;
        readonly ThreadSafeCounter BufferRemainderCounter = new ThreadSafeCounter();
        readonly ThreadSafeCounter ErrorsCounter = new ThreadSafeCounter();
        readonly EventsPrepareBuffer PrepareBuffer = new EventsPrepareBuffer();

        protected GroupRecordsWriteAdapter (string connectionString, uint writersQuantity, uint insertSize, uint transactionSize)
        {
            WritersQuantity = writersQuantity;
            Writers = new AGroupRecordsWriter[WritersQuantity];
            for (int i = 0; i < WritersQuantity; i++)
            {
                Writers[i] = createWriter( connectionString, insertSize, transactionSize );
                Writers[i].OnStored += BufferRemainderCounter.subtract;
                Writers[i].OnError += ErrorsCounter.add;
            }

            _storingThread = new StoreThread( storingIteration );
            _storingThread.start();
        }

        void storingIteration ()
        {
            List<ParameterEvent> eventsToStore = PrepareBuffer.replaceBufferIfNotEmpty();
            if (eventsToStore != null)
            {
                BufferRemainderCounter.Value = (uint)eventsToStore.Count;
                List<Exception> errors = new List<Exception>();
                uint insertedCount = storeEventsTask( eventsToStore, errors );
                OnStored?.Invoke( insertedCount, errors );

                if (BufferRemainderCounter.Value != 0)
                    Console.WriteLine( "BufferRemainderCounter.Remainder: " + BufferRemainderCounter.Value );
            }
            else {
                Thread.Sleep( 50 );
            }
        }

        protected abstract AGroupRecordsWriter createWriter (string connectionString, uint insertSize, uint transactionSize);

        public uint GetQueueLength ()
        {
            return PrepareBuffer.Length + BufferRemainderCounter.Value - ErrorsCounter.Value;
        }
        public void StoreEvents (List<ParameterEvent> eventsToStore)
        {
            PrepareBuffer.addEvents( eventsToStore );
        }

        public void WaitForStoring ()
        {
            while (Storing) {
                Thread.Sleep( 50 );
            }
        }

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
                Storing = true;
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
            finally {
                Storing = false;
            }
            return insertedCount;
        }

        Task<uint> runWriter (List<ParameterEvent> eventsToStore, AGroupRecordsWriter writer, uint startIndex, uint quantity)
        {
            IList<IGroupInsertableRecord> eventsToStoreA = eventsToStore.Cast<IGroupInsertableRecord>().ToList();

            Task <uint> task = Task.Run( () => {
                try
                {
                    Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
                    return writer.storeEvents( eventsToStoreA, startIndex, quantity );
                }
                finally 
                {
                    Thread.CurrentThread.Priority = ThreadPriority.Normal;
                }
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
                    WaitForStoring();

                    _storingThread.terminate();

                    for (int i = 0; i < WritersQuantity; i++)
                    {
                        Writers[i]?.Dispose();
                    }

                    _storingThread.waitForTermination();
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
