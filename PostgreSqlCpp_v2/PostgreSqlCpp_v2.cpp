#include <iostream>

using namespace System::Threading::Tasks;

using namespace std;
using namespace System;
using namespace System::Text;
using namespace System::Collections::Generic;
using namespace System::Threading;

using namespace PostgreSqlDataAccess;

/// <summary>
/// Вывод исключения в консоль
/// Выводит переданное исключение и всю цепочку InnerException
/// </summary>
void reportException(Exception^ error)
{
    // Отступ, для наглядности
    StringBuilder ^indentBuilder = gcnew StringBuilder( "" );

    Console::WriteLine(error->Message);
    // Console::WriteLine(error->ToString());

    // Перебираем и выводим цепочку InnerException
    Exception^ innerException = error->InnerException;
    while (innerException != nullptr) 
    {
        // Увеличиваем отступ
        indentBuilder->Append("  ");

        Console::WriteLine( indentBuilder->ToString() + innerException->Message);
        // Console::WriteLine( indentBuilder->ToString() + innerException->ToString() );
        innerException = innerException->InnerException;
    }
}

/// <summary>
/// Ссылочный класс, который содержит порцию событий для добавления
/// </summary>
ref class EventsBulk
{
private:

    /// <summary>
    /// Коллекция событий
    /// </summary>
    List< ParameterEvent^ >^ Events;

public:
    /// <summary>
    /// Конструктор порции с заданным количеством событий
    /// <param name="quantity">Количество событий</param>
    /// </summary>
    EventsBulk (int quantity) {
        Events = gcnew List< ParameterEvent^ >(quantity);
        for (int i = 0; i < quantity; i++)
        {
            ParameterEvent^ parameterEvent = gcnew ParameterEvent();
            parameterEvent->ParameterName = "parameter_" + i;
            parameterEvent->Time = DateTime::Now;
            parameterEvent->Value = 0.011F;
            parameterEvent->Status = 11;
            Events->Add(parameterEvent);
        }
    }

    /// <summary>
    /// Метод для получения коллекции событий, хранящейся в экземпляре класса
    /// </summary>
    List< ParameterEvent^ >^ getEvents() {
        return Events;
    }
};

/// <summary>
/// Метод для вывода в консоль длины очереди
/// <param name="queueLen">Количество событий в очереди</param>
/// </summary>
void logQueueLen (unsigned int queueLen)
{
    Console::Write( DateTime::Now.ToString( "HH:mm:ss.fff " ) );
    Console::WriteLine( "Queue length is " + queueLen );
}

/// <summary>
/// Метод для вывода в консоль количества добавленных записей и коллекции ошибок 
/// <param name="storedCount">Количество добавленных записей</param>
/// <param name="errors">Коллекция ошибок</param>
/// </summary>
void logErrors (unsigned int storedCount, List <Exception^> ^errors)
{
    // Если коллекция ошибок не пуста, выводим их в консоль
    if (errors != nullptr && errors->Count > 0) {
        Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
        Console::WriteLine( "There are errors!" );
        auto enumerator = errors->GetEnumerator();
        do {
            if (enumerator.Current != nullptr)
            {
                Console::WriteLine( "Error: " + enumerator.Current->Message );
            }
        } while (enumerator.MoveNext());
    }
}

/// <summary>
/// </summary>
int main()
{
    // Количество потоков добавления записей
    const int WRITERS_QUANTITY = 3;
    // Количество записей, добавляемых одним оператором INSERT
    const int INSERT_SIZE = 200;
    // Количество операций в транзакции
    const int TRANSACTION_SIZE = 10;

    // Строка подключения к БД
    // Можно формировать из конфигурационного файла, пароль запрашивать при запуске приложения
    String^ connectionString = "host=localhost;port=5432;database=Monitoring;user id=postgres;password=!Q2w3e4r;";

    // Ссылка на экземпляр класса модели БД
    MonitoringDb^ context = nullptr;

    // Объект, выполняющий отслеживание длины очереди событий
    WritingQueueLengthLogger^ logger = nullptr;

    // Объект, выполняющий добавление событий в БД
    EventsWriteAdapter^ writeAdapter = nullptr;

    try
    {
        Console::WriteLine(DateTime::Now.ToString("HH:mm:ss.fff "));

        // Подключаемся к БД
        context = gcnew MonitoringDb(connectionString);

        // Установка таймаута в секундах
        context->Database->CommandTimeout = 4;

        // EF6 создаёт БД, если её не существует
        if (context->Database->CreateIfNotExists())
            cout << "Database created" << endl;

        // Читаем стартовое количество записей в таблице событий
        int counter1 = context->ParameterEventsCount;
        Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
        Console::WriteLine("We have " + counter1 + " event(s).");

        // Счетчик количества порций 
        int counter2 = 0;

        // Размер порции событий
        int quantity = 10000;

        DateTime startTime;
        try {
            // Создаём объект, который будет выполнять добавление событий в БД
            writeAdapter = gcnew EventsWriteAdapter( connectionString, WRITERS_QUANTITY, INSERT_SIZE, TRANSACTION_SIZE);
            // Добавляем обработчик события окончания добавления порции записей
            writeAdapter->OnStored += gcnew GroupRecordsWriteAdapter::StoredEventHandler(&logErrors);

            // Создаём объект, который будет выполнять отслеживание длины очереди событий
            logger = gcnew WritingQueueLengthLogger(writeAdapter);
            // Добавляем обработчик события, которое вызывается при получении очередного значения длины очереди 
            logger->OnLogged += gcnew WritingQueueLengthLogger::LogQueueLenEventHandler(&logQueueLen);

            // Создаём порцию событий.
            // Порция используется на все операции одна, чтобы не исключить из замеров время на её создание
            EventsBulk^ bulk = gcnew EventsBulk( quantity );

            // Начинаем замер времени
            startTime = DateTime::Now;
            Console::WriteLine("Start time : " + startTime.ToString("HH:mm:ss.fff "));
            do
            {
                // Добавляем порцию в БД
                writeAdapter->StoreEvents(bulk->getEvents());

                Thread::Sleep(50);
                counter2++;

            } while ((DateTime::Now - startTime).TotalSeconds < 60);
            // До тех пор, пока не пройдет 60 секунд

            Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
            Console::WriteLine("Writing to adapter was stopped.");
        }
        finally
        {
            // Освобождаем объект, выполнявший добавление событий в БД
            // Это может занять довольно много времени, ведь нужно дождаться, когда события, накопившиеся в очереди, будут добавлены в БД
            Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
            Console::WriteLine("Disposing adapter ...");
            if (writeAdapter != nullptr)
                delete writeAdapter;

            Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
            Console::WriteLine("done.");

            // Объект, отслеживавший состояние очереди тоже освобождаем, ведь поток отслеживания нужно корректно остановить
            if (logger != nullptr)
                delete logger;
        }

        // Замеряем время окончания добавления
        DateTime endTime = DateTime::Now;
        Console::WriteLine("Writing duration is " + (endTime - startTime).TotalMilliseconds + " milliseconds.");

        // Считаем время, потраченное на запись 10000 событий
        // Чтобы определить время на запись 50000, надо просто умножить на 5.
        double timePerTenThousand = (double)(endTime - startTime).TotalMilliseconds / (counter2 * quantity / 10000);
        Console::WriteLine(timePerTenThousand.ToString("N3") + " milliseconds per 10000 events.");

        // Считаем, сколько записей теперь в таблице.
        // Тоже может занять значительное время, так как записей может быть очень много.
        // Из-за этой операции пришлось устанавливать таймаут соединения побольше стандартного
        Console::WriteLine(DateTime::Now.ToString("HH:mm:ss.fff "));
        int parametersQuantity = context->ParameterEventsCount;

        // Всего записей в таблице
        Console::WriteLine("We have " + parametersQuantity + " event(s).");
        // Считаем разницу между было и стало для определения фактического количества добавленных записей
        Console::WriteLine("We have written " + (parametersQuantity - counter1) + " event(s).");
        // Выводим количество записей, которое должно было добавиться
        Console::WriteLine("We have written " + counter2 * quantity + " event(s).");
    }
    catch (System::Exception^ error)
    {
        reportException(error);
    }
    finally
    {
        // Делаем отметки времени в консоль и закрываем соединение с БД
        Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
        Console::WriteLine("Deleting of the DB connection.");
        if (context != nullptr)
            delete context;
        Console::WriteLine("The DB connection was deleted.");
    }

    // Ждём нажатия любой клавиши
    Console::WriteLine("Press a key");
    Console::ReadKey(true);
}
