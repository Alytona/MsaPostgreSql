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
    Console::WriteLine( "Queue length is " + queueLen );
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

        writeAdapter = gcnew EventsWriteAdapter(connectionString, 3, 200, 10);
        logger = gcnew WritingQueueLengthLogger( writeAdapter );
        logger->OnLogged += gcnew WritingQueueLengthLogger::LogQueueLenEventHandler( &logQueueLen );

        int counter2 = 0;
        int quantity = 10000;
        EventsBulk^ bulk = EventsBulk::generateBulk(quantity);

        DateTime startTime = DateTime::Now;
        Console::WriteLine("Start time : " + startTime);
        do
        {
            writeAdapter->StoreEvents(bulk->getEvents());
            Thread::Sleep( 50 );
            counter2++;
        }
        while ((DateTime::Now - startTime).TotalSeconds < 60);

        while (writeAdapter->GetQueueLength() != 0) {
            Thread::Sleep(50);
        }

        Console::WriteLine("We have " + context->ParameterEventsCount + " event(s).");
        Console::WriteLine("We have written " + (context->ParameterEventsCount - counter1) + " event(s).");
        Console::WriteLine("We have written " + counter2 * 10000 + " event(s).");
    }
    catch (System::Exception^ error)
    {
        reportException(error);
    }
    finally
    {
        if (writeAdapter != nullptr)
            delete writeAdapter;

        Console::WriteLine("Deleting of the DB connection.");
        if (context != nullptr)
            delete context;
        Console::WriteLine("The DB connection was deleted.");

        if (logger != nullptr)
            delete logger;
    }

    Console::WriteLine("Press a key");
    Console::ReadKey(true);
}
