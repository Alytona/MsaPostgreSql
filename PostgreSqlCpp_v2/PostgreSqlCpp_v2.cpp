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

ref class EventsBulk
{
private:
    List< ParameterEvent^ >^ Events;

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
            parameterEvent->ParameterName = "parameter_" + i;
            parameterEvent->Time = DateTime::Now;
            parameterEvent->Value = 0.011F;
            parameterEvent->Status = 11;
            events->Add(parameterEvent);
        }
        return gcnew EventsBulk(events);
    }

    List< ParameterEvent^ >^ getEvents() {
        return Events;
    }
};

void logQueueLen(unsigned int queueLen)
{
    Console::Write( DateTime::Now.ToString( "HH:mm:ss.fff " ) );
    Console::WriteLine( "Queue length is " + queueLen );
}

void logErrors(unsigned int storedCount, List <Exception^> ^errors)
{
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

int main()
{
    String^ connectionString = "host=localhost;port=5432;database=Monitoring;user id=postgres;password=!Q2w3e4r;";

    MonitoringDb^ context = nullptr;
    WritingQueueLengthLogger^ logger = nullptr;
    EventsWriteAdapter^ writeAdapter = nullptr;

    try
    {
        context = gcnew MonitoringDb(connectionString);

        if (context->Database->CreateIfNotExists())
            cout << "Database created" << endl;

        int counter1 = context->ParameterEventsCount;
        Console::WriteLine("We have " + counter1 + " event(s).");
        int counter2 = 0;
        int quantity = 10000;

        DateTime startTime;
        try {
            writeAdapter = gcnew EventsWriteAdapter(connectionString, 3, 200, 10);
            writeAdapter->OnStored += gcnew GroupRecordsWriteAdapter::StoredEventHandler(&logErrors);
            logger = gcnew WritingQueueLengthLogger(writeAdapter);
            logger->OnLogged += gcnew WritingQueueLengthLogger::LogQueueLenEventHandler(&logQueueLen);

            counter2 = 0;
            EventsBulk^ bulk = EventsBulk::generateBulk(quantity);

            startTime = DateTime::Now;
            Console::WriteLine("Start time : " + startTime.ToString("HH:mm:ss.fff "));
            do
            {
                writeAdapter->StoreEvents(bulk->getEvents());
                Thread::Sleep(50);
                counter2++;
            } while ((DateTime::Now - startTime).TotalSeconds < 60);

            Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
            Console::WriteLine("Writing to adapter was stopped.");
        }
        finally
        {
            Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
            Console::WriteLine("Disposing adapter ...");
            if (writeAdapter != nullptr)
                delete writeAdapter;
            Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
            Console::WriteLine("done.");

            if (logger != nullptr)
                delete logger;
        }

        DateTime endTime = DateTime::Now;
        Console::WriteLine("Writing duration is " + (endTime - startTime).TotalMilliseconds + " milliseconds.");

        double timePerTenThousand = (double)(endTime - startTime).TotalMilliseconds / (counter2 * quantity / 10000);
        Console::WriteLine(timePerTenThousand.ToString("N3") + " milliseconds per 10000 events.");

        Console::WriteLine(DateTime::Now.ToString("HH:mm:ss.fff "));
        int parametersQuantity = context->ParameterEventsCount;
        Console::WriteLine("We have " + parametersQuantity + " event(s).");
        Console::WriteLine("We have written " + (parametersQuantity - counter1) + " event(s).");
        Console::WriteLine("We have written " + counter2 * quantity + " event(s).");
    }
    catch (System::Exception^ error)
    {
        reportException(error);
    }
    finally
    {
        Console::WriteLine("Deleting of the DB connection.");
        if (context != nullptr)
            delete context;
        Console::WriteLine("The DB connection was deleted.");
    }

    Console::WriteLine("Press a key");
    Console::ReadKey(true);
}
