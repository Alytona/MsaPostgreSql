// PostgreSqlCpp.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>

using namespace System::Threading::Tasks;

using namespace std;
using namespace System;
using namespace System::Collections::Generic;
using namespace System::Threading;

using namespace PostgreSqlDataAccess;

void reportException(Exception^ error)
{
    Console::WriteLine(error->Message);
    Exception^ innerException = error->InnerException;
    while (innerException != nullptr) {
        Console::WriteLine(innerException->Message);
        innerException = innerException->InnerException;
    }
}

//unsigned int storeEventsList (EventsWriteAdapter^ eventsWriteAdapter, List< ParameterEvent^ >^ events)
//{
//    List<Exception^>^ errors = gcnew List<Exception^>(0);
//    unsigned int result = eventsWriteAdapter->storeEvents(events, errors);
//    if (result != events->Count) {
//        Console::WriteLine("Some records have not written.");
//    }
//    if (errors->Count > 0)
//    {
//        Console::WriteLine("There are some errors from the writer:");
//
//        auto errorsEnumerator = errors->GetEnumerator();
//        while (errorsEnumerator.MoveNext()) {
//            Console::WriteLine("!");
//            reportException(errorsEnumerator.Current);
//        }
//        Thread::Sleep(100);
//    }
//
//    return result;
//}

ref class EventsWriter 
{
private:
    EventsWriteAdapter^ EventsWriteAdapterInstance;
    //Threading::Mutex^ WritesLock = gcnew Threading::Mutex();

    //Task <unsigned int>^ StoreTask = nullptr;

    //List< ParameterEvent^ > ^Events;

    //unsigned int storeEventsTask()
    //{
    //    List< Exception^ >^ errors = gcnew List< Exception^ >(0);
    //    unsigned int insertedCount = EventsWriteAdapterInstance->storeEvents( Events, errors );

    //    OnStored( Events->Count - insertedCount, errors );
    //    return insertedCount;
    //}

    unsigned int Quantity;
    unsigned int StoredQuantity;
    bool Stored;
    Threading::Mutex^ StoredLock = gcnew Threading::Mutex();

    void stored(unsigned int storedCount, List<Exception^>^ errors)
    {
        if (storedCount != Quantity) {
            Console::WriteLine("Some records have not written.");

            if (errors->Count > 0)
            {
                Console::WriteLine("There are some errors from the writer:");
                auto errorsEnumerator = errors->GetEnumerator();
                while (errorsEnumerator.MoveNext()) {
                    Console::WriteLine("!");
                    reportException(errorsEnumerator.Current);
                }
                Thread::Sleep(100);
            }
        }
        else
        {
            Console::WriteLine("Stored!");
        }

        setStored( true );
        StoredQuantity = storedCount;
    }

    void setStored( bool storedValue ) {
        try {
            StoredLock->WaitOne();
            Stored = storedValue;
        }
        finally
        {
            StoredLock->ReleaseMutex();
        }
    }

    bool IsStored() {
        try {
            StoredLock->WaitOne();
            return Stored;
        }
        finally
        {
            StoredLock->ReleaseMutex();
        }
    }

public:
    //delegate void StoredEventHandler(unsigned int notStoredCount, List<Exception^>^ errors);
    //event StoredEventHandler ^OnStored;

    EventsWriter(EventsWriteAdapter^ eventsWriteAdapter) {
        EventsWriteAdapterInstance = eventsWriteAdapter;
        EventsWriteAdapterInstance->OnStored += gcnew EventsWriteAdapter::StoredEventHandler(this, &EventsWriter::stored);
    }

    ~EventsWriter() {
        delete EventsWriteAdapterInstance;
    }

    void BeginStore(List< ParameterEvent^ >^ events)
    {
        Quantity = events->Count;
        setStored( false );

        EventsWriteAdapterInstance->StoreEvents(events);

        //try 
        //{
        //    // Блокировка во избежание одновременного запуска нескольких операций сохранения (в EventsWriter уже есть распределение нагрузки в несколько потоков)
        //    WritesLock->WaitOne();

        //    // Не смог победить передачу параметра в Task, пришлось использовать поле класса
        //    Events = events;
        //    StoreTask = Task<unsigned int>::Run( gcnew System::Func< unsigned int >(this, &EventsWriter::storeEventsTask) );
        //}
        //finally 
        //{
        //    WritesLock->ReleaseMutex();
        //}
    }

    unsigned int EndStore() 
    {
        while (!IsStored()) {
            Thread::Sleep( 10 );
        }
        return StoredQuantity;
    }
};


// Класс - прототип для генератора. Производит коллекцию ссобщений и обрабатывает событие сохранения коллекции
ref class EventsBulk
{
private:
    List< ParameterEvent^ >^ Events;

    //unsigned int StoredQuantity;
    //bool Stored;
    //Threading::Mutex^ StoredLock = gcnew Threading::Mutex();

    EventsBulk(List< ParameterEvent^ >^ events) {
        Events = events;
    }

public:
    static EventsBulk^ generateBulk(int quantity)
    {
        List< ParameterEvent^ >^ events = gcnew List< ParameterEvent^ >(quantity);
        for (int i = 0; i < quantity; i++)
        {
            ParameterEvent^ parameterEvent = gcnew ParameterEvent();
            parameterEvent->ParameterId = i % 10;
            parameterEvent->Time = DateTime::Now;
            parameterEvent->Value = 0.011F;
            parameterEvent->Status = 11;
            events->Add(parameterEvent);
        }
        return gcnew EventsBulk(events);
    }

    //void stored(unsigned int storedCount, List<Exception^>^ errors)
    //{
    //    if (storedCount > 0) {
    //        Console::WriteLine("Some records have not written.");

    //        if (errors->Count > 0)
    //        {
    //            Console::WriteLine("There are some errors from the writer:");
    //            auto errorsEnumerator = errors->GetEnumerator();
    //            while (errorsEnumerator.MoveNext()) {
    //                Console::WriteLine("!");
    //                reportException(errorsEnumerator.Current);
    //            }
    //            Thread::Sleep(100);
    //        }
    //    }
    //    else
    //    {
    //        Console::WriteLine("Stored!");
    //    }
    //}

    List< ParameterEvent^ >^ getEvents() {
        return Events;
    }
};


int main()
{
    String^ connectionString = "host=localhost;port=5432;database=Monitoring;user id=postgres;password=!Q2w3e4r;";

    MonitoringDb^ context = nullptr;
    try
    {
        DateTime startTime = DateTime::Now;
        Console::WriteLine("Start time : " + startTime);

        context = gcnew MonitoringDb( connectionString );

        if (context->Database->CreateIfNotExists())
            cout << "Database created" << endl;

        int counter = context->ParameterEventsCount;
        Console::WriteLine( "We have " + counter + " event(s)." );

        int quantity = 50000;
        EventsBulk^ bulk = EventsBulk::generateBulk( quantity );

        EventsWriter eventsWriter( gcnew EventsWriteAdapter(connectionString, 3, 200, 10) );

        // Назначение обработчика события
        // eventsWriter.AddOnStoredEventHandler( gcnew EventsWriteAdapter::StoredEventHandler( bulk, &EventsBulk::stored ) );

        DateTime insertsBeginTime = DateTime::Now;
        Console::WriteLine("Insert beginning time : " + insertsBeginTime);

        // Асинхронный вызов сохранения записей коллекции
        eventsWriter.BeginStore( bulk->getEvents() );

        DateTime afterAsyncCallTime = DateTime::Now;

        // Получение результата асинхронного вызова
        unsigned int result = eventsWriter.EndStore();
            
        DateTime endTime = DateTime::Now;
        Console::WriteLine("End time : " + endTime);

        TimeSpan durationTotal = endTime - startTime;
        TimeSpan durationInserts = endTime - insertsBeginTime;
        Console::WriteLine( "We wrote " + result + " records for " + durationInserts.TotalMilliseconds + " milliseconds." );
        Console::WriteLine( "Total time is : " + durationTotal.TotalMilliseconds + " milliseconds.");

        Console::WriteLine("BeginStore - EndStore duration : " + (afterAsyncCallTime - insertsBeginTime).TotalMilliseconds + " milliseconds.");

        if (result == quantity)
            Console::WriteLine("Everything was fine!");
    }
    catch (System::Exception ^error)
    {
        reportException(error);
    }
    finally
    {
        if (context != nullptr)
            delete context;
    }

    Console::WriteLine( "Press a key" );
    Console::ReadKey(true);
}
