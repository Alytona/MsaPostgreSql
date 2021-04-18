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
        /// <summary>
        /// Блокировка для потокобезопасного доступа к счетчику
        /// </summary>
        readonly object ValueLock = new object();

        /// <summary>
        /// Значение счётчика
        /// </summary>
        public uint _value;

        /// <summary>
        /// Свойство, обеспечивающее потокобезопасный доступ к счетчику
        /// </summary>
        public uint Value
        {
            get {
                lock (ValueLock) { return _value; }
            }
            set {
                lock (ValueLock) { _value = value; }
            }
        }

        /// <summary>
        /// Вычесть заданное количество из счётчика
        /// </summary>
        /// <param name="quantity">Количество, которое следует вычесть из счётчика</param>
        public void subtract (uint quantity)
        {
            lock (ValueLock) { 
                _value -= quantity; 
            }
        }
        /// <summary>
        /// Добавить заданное количество к счётчику
        /// </summary>
        /// <param name="quantity">Количество, которое следует добавить к счётчику</param>
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
        /// <summary>
        /// Коллекция, в которой хранятся записи буфера
        /// </summary>
        List<ParameterEvent> _events = new List<ParameterEvent>();
        /// <summary>
        /// Блокировка для потокобезопасного доступа к буферу
        /// </summary>
        readonly object _eventsLock = new object();

        /// <summary>
        /// Количество записей в буфере
        /// </summary>
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

        /// <summary>
        /// Метод создаёт новый буфер событий и возвращает ссылку на старый
        /// Используется для переключения процессов записи в БД на накопленный буфер
        /// Если старый буфер пуст, то просто возвращается null
        /// </summary>
        /// <returns>Ссылка на старый буфер</returns>
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
        /// <summary>
        /// Добавить события в буфер
        /// </summary>
        /// <param name="eventsToStore">Коллекция записей, которые нужно добавить</param>
        public void addEvents (List<ParameterEvent> eventsToStore)
        {
            lock (_eventsLock)
            {
                _events.AddRange( eventsToStore );
            }
        }
    }

    /// <summary>
    /// Класс-контейнер для потока, в котором будет работать GroupRecordsWriteAdapter
    /// Обеспечивает управляемый останов, с ожиданием самостоятельного завершения потока
    /// </summary>
    class StoreThread
    {
        /// <summary>
        /// Возможные состояния потока
        /// </summary>
        enum StoreThreadStates
        {
            Working,        // Работаем
            Terminating,    // Пора заканчивать
            Terminated      // Закончили
        }
        /// <summary>
        /// Текущее состояние потока
        /// </summary>
        StoreThreadStates _storeThreadState;

        /// <summary>
        /// Блокировка для исключения конфликтов чтения-записи состояния потока из разных потоков
        /// </summary>
        readonly object _storeThreadStateLock = new object();

        /// <summary>
        /// Рабочий поток
        /// </summary>
        readonly Thread _storeThread;

        /// <summary>
        /// Объект, содержащий логику рабочего потока
        /// </summary>
        readonly ThreadStart _storeLogic;

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="storeLogic">Объект, содержащий логику рабочего потока</param>
        public StoreThread (ThreadStart storeLogic)
        {
            _storeLogic = storeLogic;

            // Создаём рабочий поток
            _storeThreadState = StoreThreadStates.Working;
            _storeThread = new Thread( threadMethod );

            // Приоритет назначаем ниже обычного
            _storeThread.Priority = ThreadPriority.BelowNormal;
        }

        /// <summary>
        /// Запуск рабочего потока
        /// </summary>
        public void start ()
        {
            _storeThread.Start();
        }
        /// <summary>
        /// Сигнализирует потоку, что пора заканчивать
        /// </summary>
        public void terminate ()
        {
            lock (_storeThreadStateLock)
            {
                _storeThreadState = StoreThreadStates.Terminating;
            }
        }
        /// <summary>
        /// Ожидание завершения потока.
        /// По истечении таймаута, если таймаут не равен нулю, поток завершается принудительно.
        /// </summary>
        /// <param name="timeout">Таймаут останова в секундах.</param>
        public void waitForTermination (uint timeout = 0)
        {
            if (timeout == 0)
                _storeThread.Join();
            else {
                if (!_storeThread.Join( (int)timeout * 1000 ))
                {
                    _storeThread.Abort();
                }
            }
        }

        /// <summary>
        /// Тело рабочего потока
        /// </summary>
        void threadMethod ()
        {
            StoreThreadStates threadState;
            do
            {
                // Выполняем логику итерации потока
                _storeLogic.Invoke();

                // И проверяем, не пора ли завершаться
                lock (_storeThreadStateLock)
                {
                    threadState = _storeThreadState;
                }
            }
            while (threadState == StoreThreadStates.Working);

            // После завершения устанавливаем признак, что работа закончена
            lock (_storeThreadStateLock)
            {
                _storeThreadState = StoreThreadStates.Terminated;
            }
        }
    }

    public abstract class GroupRecordsWriteAdapter : IDisposable
    {
        /// <summary>
        /// Массив потоков добавления записей в БД
        /// </summary>
        readonly AGroupRecordsWriter[] Writers;
        /// <summary>
        /// Количество потоков добавления записей в БД
        /// </summary>
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

        /// <summary>
        /// Признак того, что выполняется запись буфера в БД.
        /// Используется при ожидании завершения записи перед освобождением ресурсов
        /// </summary>
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

        /// <summary>
        /// Счетчик ошибок при записи в БД
        /// </summary>
        readonly ThreadSafeCounter ErrorsCounter = new ThreadSafeCounter();
        /// <summary>
        /// Счетчик объектов, ожидающих записи в БД в буфере сохранения
        /// </summary>
        readonly ThreadSafeCounter BufferRemainderCounter = new ThreadSafeCounter();
        /// <summary>
        /// Накопительный буфер объектов, ожидающих записи в БД
        /// </summary>
        readonly EventsPrepareBuffer PrepareBuffer = new EventsPrepareBuffer();

        /// <summary>
        /// Основной поток
        /// </summary>
        readonly StoreThread _storingThread;

        /// <summary>
        /// Логика итерации основного потока
        /// </summary>
        void storingIteration ()
        {
            // Переключаем буферы - в накопительном буфере создаём новую коллекцию, а для того, что накопилось вызываем сохранение
            List<ParameterEvent> eventsToStore = PrepareBuffer.replaceBufferIfNotEmpty();

            // Если буфер не пуст
            if (eventsToStore != null)
            {
                // Инициализируем количество несохраненных записей в буфере сохранения
                BufferRemainderCounter.Value = (uint)eventsToStore.Count;
                List<Exception> errors = new List<Exception>();

                // Выполняем сохранение в синхронном режиме
                uint insertedCount = storeEventsTask( eventsToStore, errors );
                // Сообщаем о результатах сохранения
                OnStored?.Invoke( insertedCount, errors );

                // Если по каким-то причинам что-то осталось в буфере сохранения, сообщаем об этом
                if (BufferRemainderCounter.Value != 0)
                    Console.WriteLine( "BufferRemainderCounter.Remainder: " + BufferRemainderCounter.Value );
            }
            else
            {
                // Если буфер был пустым, надо выполнить задержку
                Thread.Sleep( 50 );
            }
        }

        /// <summary>
        /// Констуктор
        /// </summary>
        /// <param name="connectionString">Строка подключения к БД</param>
        /// <param name="writersQuantity">Количество потоков добавления записей</param>
        /// <param name="insertSize">Количество записей, добавляемых одним оператором INSERT</param>
        /// <param name="transactionSize">Количество операций в транзакции</param>
        protected GroupRecordsWriteAdapter (string connectionString, uint writersQuantity, uint insertSize, uint transactionSize)
        {
            // Создаём объекты, которые будут выполнять запись объектов в БД
            WritersQuantity = writersQuantity;
            Writers = new AGroupRecordsWriter[WritersQuantity];
            for (int i = 0; i < WritersQuantity; i++)
            {
                Writers[i] = createWriter( connectionString, insertSize, transactionSize );
                // Когда писатель запишет порцию в БД, счетчик оставшихся записей будет уменьшен на количество записанных
                Writers[i].OnStored += BufferRemainderCounter.subtract;
                // В случае ошибок будет увеличен счетчик ошибок
                Writers[i].OnError += ErrorsCounter.add;
            }

            // Создаём и запускаем поток накопления и сохранения
            _storingThread = new StoreThread( storingIteration );
            _storingThread.start();
        }

        /// <summary>
        /// Метод создания потока сохранения объектов в БД
        /// </summary>
        /// <param name="connectionString">Строка подключения к БД</param>
        /// <param name="insertSize">Количество записей, добавляемых одним оператором INSERT</param>
        /// <param name="transactionSize">Количество операций в транзакции</param>
        /// <returns></returns>
        protected abstract AGroupRecordsWriter createWriter (string connectionString, uint insertSize, uint transactionSize);

        /// <summary>
        /// Вычисляет количество объектов, ожидающих записи в БД
        /// </summary>
        /// <returns>Количество объектов, ожидающих записи в БД</returns>
        public uint GetQueueLength ()
        {
            // Считается как количество записей в накопительном буфере и в сохраняемом буфере, минус количество ошибок записи
            return PrepareBuffer.Length + BufferRemainderCounter.Value - ErrorsCounter.Value;
        }

        /// <summary>
        /// Добавляет записи в накопительный буфер
        /// </summary>
        /// <param name="eventsToStore">Коллекция записей для добавления</param>
        public void StoreEvents (List<ParameterEvent> eventsToStore)
        {
            PrepareBuffer.addEvents( eventsToStore );
        }

        /// <summary>
        /// Ожидание завершения записи буфера сохранения
        /// </summary>
        public void WaitForStoring ()
        {
            while (Storing) {
                Thread.Sleep( 50 );
            }
        }

        /// <summary>
        /// Запись буфера сохранения в БД
        /// </summary>
        /// <param name="eventsToStore">Буфер сохранения</param>
        /// <param name="errors">Коллекция ошибок, возникших при сохранении</param>
        /// <returns>Количество записей, добавленных в БД</returns>
        uint storeEventsTask (List<ParameterEvent> eventsToStore, List<Exception> errors)
        {
            uint totalQuantity = (uint)eventsToStore.Count;

            // Считаем количество записей, приходящееся на один писатель
            uint baseQuantityPerWriter = (totalQuantity - 1) / WritersQuantity;

            // Считаем, на сколько писателей придётся на одну запись больше
            uint remainder = (totalQuantity - 1) - baseQuantityPerWriter * WritersQuantity;

            // Индекс в коллекции, с которого текущий писатель начнёт сохранение
            uint startIndex = 0;

            // Количество добавленных записей
            uint insertedCount = 0;

            // Коллекция заданий
            List<Task<uint>> tasks = new List<Task<uint>>();
            try
            {
                // Поднимаем флаг записи буфера
                Storing = true;

                for (int i = 0; i < WritersQuantity; i++)
                {
                    // Определяем количество записей, которое должен будет сохранить этот писатель
                    uint quantity = baseQuantityPerWriter;
                    if (i <= remainder)
                        quantity++;
                    if (quantity == 0)
                        break;

                    // Создаём задание для писателя
                    tasks.Add( runWriter( eventsToStore, Writers[i], startIndex, quantity ) );

                    // Сдвигаем стартовый индекс
                    startIndex += quantity;
                }

                // Ожидаем завершения заданий и собираем количество сохраненных записей
                foreach (Task<uint> task in tasks)
                {
                    task.Wait();
                    insertedCount += task.Result;
                }
            }
            catch (Exception error)
            {
                // Если произошла ошибка, добавляем её в коллекцию ошибок
                errors.Add( error );

                // Если ошибка произошла в каком-то задании, тоже добавляем её в коллекцию ошибок
                foreach (Task<uint> task in tasks)
                {
                    if (task.Exception != null)
                        errors.Add( task.Exception );
                }
            }
            finally {
                // Опускаем флаг записи в буфера сохранения
                Storing = false;
            }
            return insertedCount;
        }

        /// <summary>
        /// Создание задания для писателя (объекта, выполняющего сохранение записей в БД)
        /// </summary>
        /// <param name="eventsToStore">Коллекция записей для добавления</param>
        /// <param name="writer">Ссылка на писателя</param>
        /// <param name="startIndex">Индекс записи, с которой нужно начать добавление</param>
        /// <param name="quantity">Количество записей, которые нужно добавить</param>
        /// <returns>Созданное задание</returns>
        Task<uint> runWriter (List<ParameterEvent> eventsToStore, AGroupRecordsWriter writer, uint startIndex, uint quantity)
        {
            // Приведение типа коллекции
            IList<IGroupInsertableRecord> eventsToStoreA = eventsToStore.Cast<IGroupInsertableRecord>().ToList();

            Task <uint> task = Task.Run( () => {
                try
                {
                    // Устанавливаем приоритет потока ниже обычного
                    Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;

                    // Вызываем метод сохранения писателя
                    return writer.storeEvents( eventsToStoreA, startIndex, quantity );
                }
                finally 
                {
                    // Восстанавливаем приоритет потока
                    Thread.CurrentThread.Priority = ThreadPriority.Normal;
                }
            } );
            return task;
        }

        #region Поддержка интерфейса IDisposable, освобождение неуправляемых ресурсов

        private bool disposedValue = false; // Для определения излишних вызовов, чтобы выполнять Dispose только один раз

        /// <summary>
        /// Метод, выполняющий освобождение неуправляемых ресурсов
        /// </summary>
        /// <param name="disposing">Признак того, что вызов метода выполнен не из финализатора</param>
        protected virtual void Dispose (bool disposing)
        {
            // Если Dispose ещё не вызывался
            if (!disposedValue)
            {
                // Если вызов выполнен не из финализатора
                if (disposing)
                {
                    // Ждем окончания записи буфера сохранения
                    WaitForStoring();

                    // Сообщаем основному потоку, что надо заканчиваться
                    _storingThread.terminate();

                    // Останавливаем писателей
                    for (int i = 0; i < WritersQuantity; i++)
                    {
                        Writers[i]?.Dispose();
                    }

                    // Ждём завершения основного потока
                    _storingThread.waitForTermination();
                }
                // Больше не выполнять
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
