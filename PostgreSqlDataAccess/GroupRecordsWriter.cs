using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgreSqlDataAccess
{
    /// <summary>
    /// Базовый класс объектов, инкапсулирующих потоки добавления записей
    /// </summary>
    public abstract class AGroupRecordsWriter : IDisposable
    {
        MonitoringDb DbContext;

        bool DbInited = false;

        readonly uint TransactionSize;

        public string ConnectionString
        {
            get; private set;
        }

        readonly AGroupInsertMaker InsertMaker;

        protected AGroupRecordsWriter (string connectionString, AGroupInsertMaker insertMaker, uint transactionSize)
        {
            ConnectionString = connectionString;
            TransactionSize = transactionSize;

            InsertMaker = insertMaker;
        }

        public uint storeEvents (IList<IGroupInsertableRecord> recordsToStore, uint startIndex, uint quantity)
        {
            uint insertResultCounter = 0;

            if (!DbInited)
                dbInit();

            InsertMaker.setCollection( recordsToStore, startIndex, quantity );

            uint insertsCounter = 0;
            string query = InsertMaker.nextQuery();
            while (query != null)
            {
                int queryResult = DbContext.Database.ExecuteSqlCommand( query, InsertMaker.FieldValues );
                if (queryResult > 0)
                    insertResultCounter += (uint)queryResult;

                if (++insertsCounter == TransactionSize)
                {
                    DbContext.SaveChanges();
                    insertsCounter = 0;
                }

                query = InsertMaker.nextQuery();
            }

            if (insertsCounter > 0)
                DbContext.SaveChanges();

            return insertResultCounter;
        }

        void dbInit ()
        {
            DbContext = new MonitoringDb( ConnectionString );
            DbInited = true;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose (bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (DbInited)
                    {
                        DbContext.Dispose();
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
