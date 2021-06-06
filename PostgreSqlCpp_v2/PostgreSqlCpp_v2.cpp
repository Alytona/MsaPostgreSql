#include <iostream>

using namespace System::Threading::Tasks;

using namespace std;
using namespace System;
using namespace System::Text;
using namespace System::Collections::Generic;
using namespace System::Threading;

using namespace PostgreSqlDataAccess;

/// <summary>
/// ����� ���������� � �������
/// ������� ���������� ���������� � ��� ������� InnerException
/// </summary>
void reportException(Exception^ error)
{
    // ������, ��� �����������
    StringBuilder ^indentBuilder = gcnew StringBuilder( "" );

    Console::WriteLine(error->Message);
    // Console::WriteLine(error->ToString());

    // ���������� � ������� ������� InnerException
    Exception^ innerException = error->InnerException;
    while (innerException != nullptr) 
    {
        // ����������� ������
        indentBuilder->Append("  ");

        Console::WriteLine( indentBuilder->ToString() + innerException->Message);
        // Console::WriteLine( indentBuilder->ToString() + innerException->ToString() );
        innerException = innerException->InnerException;
    }
}

/// <summary>
/// ��������� �����, ������� �������� ������ ������� ��� ����������
/// </summary>
ref class EventsBulk
{
private:

    /// <summary>
    /// ��������� �������
    /// </summary>
    List< ParameterEvent^ >^ Events;

public:
    /// <summary>
    /// ����������� ������ � �������� ����������� �������
    /// <param name="quantity">���������� �������</param>
    /// </summary>
/*
    EventsBulk (int quantity) {

        int portionQuantity = quantity / 6;

        DateTime currentTime = DateTime::Now;
        Events = gcnew List< ParameterEvent^ >(quantity);
        int i = 0;
        for (; i < portionQuantity * 2 + 500; i++)
        {
            ParameterEvent^ parameterEvent = gcnew ParameterEvent();
            parameterEvent->ParameterId = 1; // i % 10;
            parameterEvent->Time = DateTime::Now; // currentTime;
            parameterEvent->Value = 0.011F;
            parameterEvent->Status = 11;
            Events->Add(parameterEvent);
        }
        for (; i < portionQuantity * 4 + 700; i++)
        {
            ParameterEvent^ parameterEvent = gcnew ParameterEvent();
            parameterEvent->ParameterId = 2; // i % 10;
            parameterEvent->Time = DateTime::Now; // currentTime;
            parameterEvent->Value = 0.011F;
            parameterEvent->Status = 11;
            Events->Add(parameterEvent);
        }
        for (; i < portionQuantity * 5; i++)
        {
            ParameterEvent^ parameterEvent = gcnew ParameterEvent();
            parameterEvent->ParameterId = 3; // i % 10;
            parameterEvent->Time = DateTime::Now; // currentTime;
            parameterEvent->Value = 0.011F;
            parameterEvent->Status = 11;
            Events->Add(parameterEvent);
        }
        for (; i < portionQuantity * 5 + 1000; i++)
        {
            ParameterEvent^ parameterEvent = gcnew ParameterEvent();
            parameterEvent->ParameterId = 6; // i % 10;
            parameterEvent->Time = DateTime::Now; // currentTime;
            parameterEvent->Value = 0.011F;
            parameterEvent->Status = 11;
            Events->Add(parameterEvent);
        }
        for (; i < quantity; i++)
        {
            ParameterEvent^ parameterEvent = gcnew ParameterEvent();
            parameterEvent->ParameterId = 7; // i % 10;
            parameterEvent->Time = DateTime::Now; // currentTime;
            parameterEvent->Value = 0.011F;
            parameterEvent->Status = 11;
            Events->Add(parameterEvent);
        }
    }
*/
    EventsBulk(int quantity) {

        DateTime currentTime = DateTime::Now;
        Events = gcnew List< ParameterEvent^ >(quantity);
        for (int i = 0; i < quantity; i++)
        {
            ParameterEvent^ parameterEvent = gcnew ParameterEvent();
            parameterEvent->ParameterId = i % 1000;
            parameterEvent->Time = DateTime::Now; // currentTime;
            parameterEvent->Value = 0.011F;
            parameterEvent->Status = 11;
            Events->Add(parameterEvent);
            // currentTime = currentTime.AddMinutes(30);
        }
    }

    /// <summary>
    /// ����� ��� ��������� ��������� �������, ���������� � ���������� ������
    /// </summary>
    List< ParameterEvent^ >^ getEvents() {
        return Events;
    }
};

/// <summary>
/// ����� ��� ������ � ������� ����� �������
/// <param name="queueLen">���������� ������� � �������</param>
/// </summary>
void logQueueLen (unsigned int preparedQueueLen, unsigned int storingQueueLen, unsigned int errorsQuantity )
{
    Console::Write( DateTime::Now.ToString( "HH:mm:ss.fff " ) );
    // Console::WriteLine( "Queue length is " + (preparedQueueLen + storingQueueLen - errorsQuantity));
    Console::WriteLine( "Prepared events : " + preparedQueueLen + ", storing events : " + storingQueueLen + ", errors : " + errorsQuantity );
}

/// <summary>
/// ����� ��� ������ � ������� ���������� ����������� ������� � ��������� ������ 
/// <param name="storedCount">���������� ����������� �������</param>
/// <param name="errors">��������� ������</param>
/// </summary>
void logErrors (unsigned int storedCount, List <Exception^> ^errors)
{
    // ���� ��������� ������ �� �����, ������� �� � �������
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
    // ���������� ������� ���������� �������
    const int WRITERS_QUANTITY = 3;
    // ���������� �������, ����������� ����� ���������� INSERT
    const int INSERT_SIZE = 200;
    // ���������� �������� � ����������
    const int TRANSACTION_SIZE = 10;

    // ������ ����������� � ��
    // ����� ����������� �� ����������������� �����, ������ ����������� ��� ������� ����������
    String^ connectionString = "host=localhost;port=5432;database=Monitoring_V2;user id=postgres;password=!Q2w3e4r;";

    // ������ �� ��������� ������ ������ ��
    MonitoringDb^ context = nullptr;

    // ������, ����������� ������������ ����� ������� �������
    WritingQueueLengthLogger^ logger = nullptr;

    // ������, ����������� ���������� ������� � ��
    EventsWriteAdapter^ writeAdapter = nullptr;

    try
    {
        Console::WriteLine(DateTime::Now.ToString("HH:mm:ss.fff "));

        // ������������ � ��
        context = gcnew MonitoringDb(connectionString);

        // ��������� �������� � ��������
        context->Database->CommandTimeout = 30;

        // EF6 ������ ��, ���� � �� ����������
        if (context->Database->CreateIfNotExists())
            cout << "Database created" << endl;

        // ���������� ������� ����������
        //for (int i = 1; i <= 1000; i++) 
        //{
        //    Parameter^ parameter = gcnew Parameter();
        //    parameter->Name = "parameter " + i;
        //    parameter->Comment = "����������� � ���������";
        //    parameter->Units = "inches, ����� ��-������";
        //    parameter->Source = "������ �� ��������";
        //    context->Parameters->Add(parameter);
        //}
        //context->SaveChanges();

        //int startEventId = context->LastParameterEventId.HasValue ? context->LastParameterEventId.Value : -1;
        //Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
        //Console::WriteLine("Last id : " + startEventId);

        // ������ ��������� ���������� ������� � ������� �������
        int counter1 = context->ParameterEventsCount;
        Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
        Console::WriteLine("We have " + counter1 + " event(s).");

        // ������� ���������� ������ 
        int counter2 = 0;

        // ������ ������ �������
        int quantity = 10000;

        DateTime startTime;
        try {
            // ������ ������, ������� ����� ��������� ���������� ������� � ��
            writeAdapter = gcnew EventsWriteAdapter( connectionString, WRITERS_QUANTITY, INSERT_SIZE, TRANSACTION_SIZE);
            // ��������� ���������� ������� ��������� ���������� ������ �������
            writeAdapter->OnStored += gcnew GroupRecordsWriteAdapter::StoredEventHandler(&logErrors);

            // ������ ������, ������� ����� ��������� ������������ ����� ������� �������
            logger = gcnew WritingQueueLengthLogger(writeAdapter);
            // ��������� ���������� �������, ������� ���������� ��� ��������� ���������� �������� ����� ������� 
            logger->OnLogged += gcnew WritingQueueLengthLogger::LogQueueLenEventHandler(&logQueueLen);

            // ������ ������ �������.
            // ������ ������������ �� ��� �������� ����, ����� �� ��������� �� ������� ����� �� � ��������
            EventsBulk^ bulk = gcnew EventsBulk( quantity );

            // �������� ����� �������
            startTime = DateTime::Now;
            Console::WriteLine("Start time : " + startTime.ToString("HH:mm:ss.fff "));
            do
            {
                // ��������� ������ � ��
                writeAdapter->StoreEvents(bulk->getEvents());

                Thread::Sleep(50);
                counter2++;

                // break;

            } while ((DateTime::Now - startTime).TotalSeconds < 60);
            // �� ��� ���, ���� �� ������� 60 ������

            Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
            Console::WriteLine("Writing to adapter was stopped.");
        }
        finally
        {
            // ����������� ������, ����������� ���������� ������� � ��
            // ��� ����� ������ �������� ����� �������, ���� ����� ���������, ����� �������, ������������ � �������, ����� ��������� � ��
            Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
            Console::WriteLine("Disposing adapter ...");
            if (writeAdapter != nullptr)
                delete writeAdapter;

            Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
            Console::WriteLine("done.");

            // ������, ������������� ��������� ������� ���� �����������, ���� ����� ������������ ����� ��������� ����������
            if (logger != nullptr)
                delete logger;
        }

        // �������� ����� ��������� ����������
        DateTime endTime = DateTime::Now;
        Console::WriteLine("Writing duration is " + (endTime - startTime).TotalMilliseconds + " milliseconds.");

        // ������� �����, ����������� �� ������ 10000 �������
        // ����� ���������� ����� �� ������ 50000, ���� ������ �������� �� 5.
        double timePerTenThousand = (double)(endTime - startTime).TotalMilliseconds / (counter2 * quantity / 10000);
        Console::WriteLine(timePerTenThousand.ToString("N3") + " milliseconds per 10000 events.");

        // �������, ������� ������� ������ � �������.
        // ���� ����� ������ ������������ �����, ��� ��� ������� ����� ���� ����� �����.
        // ��-�� ���� �������� �������� ������������� ������� ���������� �������� ������������
        Console::WriteLine(DateTime::Now.ToString("HH:mm:ss.fff "));
        int parametersQuantity = context->ParameterEventsCount;

        //int endEventId = context->LastParameterEventId;
        //Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
        //Console::WriteLine("Last id : " + endEventId);

        // ����� ������� � �������
        Console::WriteLine("We have " + parametersQuantity + " event(s).");
        // ������� ������� ����� ���� � ����� ��� ����������� ������������ ���������� ����������� �������
        Console::WriteLine("We have written " + (parametersQuantity - counter1) + " event(s).");
        // Console::WriteLine("We have written " + (endEventId - startEventId) + " event(s).");
        // ������� ���������� �������, ������� ������ ���� ����������
        Console::WriteLine("We have written " + counter2 * quantity + " event(s).");
    }
    catch (System::Exception^ error)
    {
        reportException(error);
    }
    finally
    {
        // ������ ������� ������� � ������� � ��������� ���������� � ��
        Console::Write(DateTime::Now.ToString("HH:mm:ss.fff "));
        Console::WriteLine("Deleting of the DB connection.");
        if (context != nullptr)
            delete context;
        Console::WriteLine("The DB connection was deleted.");
    }

    // ��� ������� ����� �������
    Console::WriteLine("Press a key");
    Console::ReadKey(true);
}
