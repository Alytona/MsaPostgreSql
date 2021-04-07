using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgreSqlDataAccess
{
    public abstract class AGroupInsertMaker
    {
        readonly uint ColumnsQuantity;
        readonly string InsertQuery;
        public readonly uint InsertSize;

        readonly string[] ValuesParts;
        public readonly object[] FieldValues;

        IList<IGroupInsertableRecord> RecordsToStore;
        uint StartIndex;
        uint RowIndex;
        uint RecordsQuantity;

        protected AGroupInsertMaker (uint columnsQuantity, string insertQuery, uint insertSize)
        {
            InsertQuery = insertQuery;
            ColumnsQuantity = columnsQuantity;
            InsertSize = insertSize;

            ValuesParts = new string[InsertSize];
            string[] formatParts = new string[ColumnsQuantity];
            for (int i = 0; i < InsertSize; i++)
            {
                object[] values = new object[ColumnsQuantity];
                for (int j = 0; j < ColumnsQuantity; j++)
                {
                    uint cellIndex = (uint)(i * ColumnsQuantity + j);
                    values[j] = cellIndex;
                    formatParts[j] = "@p" + cellIndex;
                }
                ValuesParts[i] = "(" + string.Join( ", ", formatParts ) + ")";
            }
            FieldValues = new object[InsertSize * ColumnsQuantity];
        }

        public void setCollection (IList<IGroupInsertableRecord> recordsToStore, uint startIndex, uint quantity)
        {
            //checkCollectionType( eventsToStore );

            RecordsToStore = recordsToStore;
            StartIndex = startIndex;
            RowIndex = StartIndex;
            RecordsQuantity = quantity;
        }

        public string nextQuery ()
        {
            if (RowIndex >= StartIndex + RecordsQuantity)
                return null;

            StringBuilder queryBuilder = new StringBuilder( InsertQuery );

            uint insertRowIndex = 0;
            uint valuesIndex = 0;

            while (RowIndex < StartIndex + RecordsQuantity)
            {
                queryBuilder.Append( ValuesParts[insertRowIndex] );

                RecordsToStore[(int)RowIndex].FillValues( FieldValues, valuesIndex );
                valuesIndex += ColumnsQuantity;

                insertRowIndex++;
                RowIndex++;
                if (insertRowIndex == InsertSize || RowIndex == StartIndex + RecordsQuantity)
                {
                    queryBuilder.Append( ";" );
                    break;
                }
                else
                {
                    queryBuilder.Append( ", " );
                }
            }
            return queryBuilder.ToString();
        }

        protected abstract void checkCollectionType (IList<IGroupInsertableRecord> eventsToStore);
    }
}
