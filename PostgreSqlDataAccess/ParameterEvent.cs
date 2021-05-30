﻿using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgreSqlDataAccess
{
    /// <summary>
    /// Запись, которая умеет заполнять object[] значениями своих полей
    /// </summary>
    public interface IGroupInsertableRecord
    {
        void FillValues (object[] fieldValues, uint valuesIndex);
    }

    /// <summary>
    /// Класс записи события
    /// </summary>
    public class ParameterEvent : IGroupInsertableRecord
    {
        // Следующие статические члены класса используются при построении запроса для группового добавления записей
        /// <summary>
        /// Количество колонок
        /// </summary>
        internal static readonly uint ColumnsQuantity = 4;
        /// <summary>
        /// Начальная часть SQL-оператора добавления записей
        /// </summary>
        //internal static readonly string InsertQuery = "insert into \"ParameterEvents\" (par_id, event_time, event_value, event_status) values ";
        /// <summary>
        /// Начальная часть SQL-оператора добавления записей
        /// </summary>
        // internal static readonly string InsertQueryFormat = "insert into \"parameter{0}values\" (year_month, event_time, event_value, event_status) values ";
        /// <summary>
        /// Строка формата для заполнения значений одной из вставляемых записей
        /// </summary>
        internal static readonly string ValuesPartFormat = "(@p{0}, @p{1}, @p{2}, @p{3})";

        //[Column( "year_month" )]
        public int ParameterId
        {
            get; set;
        }

        /// <summary>
        /// Идентификатор события, суррогатный ключ с автоинкрементом
        /// </summary>
        // [Key]
        [Column( "year_month" )]
        public string YearMonth
        {
            get {
                return $"{ Time.Year - 2000 }_{ Time.Month:D2}";
            }
        }

        /// <summary>
        /// Идентификатор события, суррогатный ключ с автоинкрементом
        /// </summary>
        // [Key]
        [Column( "event_id" )]
        public int EventId
        {
            get; set;
        }
        /// <summary>
        /// Время появления события
        /// </summary>
        [Column( "event_time" )]
        public DateTime Time
        {
            get; set;
        }
        /// <summary>
        /// Значение, ассоциированное с событием
        /// </summary>
        [Column( "event_value" )]
        public float Value
        {
            get; set;
        }
        /// <summary>
        /// Состояние события
        /// </summary>
        [Column( "event_status" )]
        public int Status
        {
            get; set;
        }

        /// <summary>
        /// Метод для отображения в виде строки
        /// </summary>
        /// <returns></returns>
        public override string ToString ()
        {
            return YearMonth + " - " + EventId;
        }

        /// <summary>
        /// Метод заполнения массива со значениями полей записи.
        /// Заполняется начиная с указанного индекса. 
        /// Массив используется для передачи значений полей добавляемых записей в метод добавления записей. 
        /// </summary>
        /// <param name="fieldValues">Массив, куда будут помещены значения</param>
        /// <param name="valuesIndex">Индекс в массиве, начиная с которого вписываются значения</param>
        public void FillValues (object[] fieldValues, uint valuesIndex)
        {
            fieldValues[valuesIndex] = YearMonth;
            fieldValues[valuesIndex + 1] = Time;
            fieldValues[valuesIndex + 2] = Value;
            fieldValues[valuesIndex + 3] = Status;
        }
    }

    /// <summary>
    /// Реализация построителя группового оператора insert для таблицы ParameterEvents
    /// </summary>
    class EventsGroupInsertMaker : AGroupInsertMaker
    {
        public int ParameterId
        {
            get; set;
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="insertSize">Максимальное количество записей, добавляемых одним оператором insert</param>
        public EventsGroupInsertMaker (uint insertSize) : base( ParameterEvent.ColumnsQuantity, insertSize )
        {
        }

        /// <summary>
        /// Метод проверки типа записей в коллекции перед добавлением
        /// </summary>
        /// <param name="eventsToStore">Коллекция записей, подготовленная для добавления</param>
        protected override void checkCollectionType (IList<IGroupInsertableRecord> eventsToStore)
        {
            if (eventsToStore is IList<ParameterEvent>)
                return;

            throw new ArrayTypeMismatchException( "Передана коллекция с неверным типом записей" );
        }

        protected override StringBuilder makeQueryBuilder ()
        {
            StringBuilder queryBuilder = new StringBuilder( "insert into \"parameter" + ParameterId + "values\" (year_month, event_time, event_value, event_status) values " );
            return queryBuilder;
        }

    }

    /// <summary>
    /// Реализация объекта, инкапсулирующего поток добавления записей
    /// </summary>
    class EventsGroupRecordsWriter : AGroupRecordsWriter
    {
        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="connectionString">Строка с параметрами соединения с сервером БД</param>
        /// <param name="insertSize">Максимальное количество записей, добавляемых одним оператором insert</param>
        /// <param name="transactionSize">Максимальное количество операторов insert в одной транзакции</param>
        public EventsGroupRecordsWriter (string connectionString, uint insertSize, uint transactionSize) : base( connectionString, new EventsGroupInsertMaker( insertSize ), transactionSize )
        {
        }

        public void setParameterId (int parameterId)
        {
            if (InsertMaker is EventsGroupInsertMaker insertMaker) {
                insertMaker.ParameterId = parameterId;
            }
        }
    }
}
