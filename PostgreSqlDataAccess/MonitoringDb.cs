using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgreSqlDataAccess
{
    /// <summary>
    /// Модель БД мониторинга
    /// </summary>
    public class MonitoringDb : DbContext
    {
        /// <summary>
        /// Таблица событий
        /// </summary>
        public DbSet<ParameterEvent> ParameterEvents
        {
            get; set;
        }
        /// <summary>
        /// Количество записей в таблице событий.
        /// Сделано отдельное свойство, так как Count - метод расширения и в проектах на C++\CLI недоступен 
        /// </summary>
        public int ParameterEventsCount
        {
            get => ParameterEvents.Count();
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="connectionString">Строка с параметрами соединения с сервером БД</param>
        public MonitoringDb (string connectionString) : base( new Npgsql.NpgsqlConnection( connectionString ), contextOwnsConnection: true )
        {
            // Выключаем автоматический запуск DetectChanges()
            Configuration.AutoDetectChangesEnabled = false;
            // Выключаем автоматическую валидацию при вызове SaveChanges()
            Configuration.ValidateOnSaveEnabled = false;
            // Выключаем создание прокси-экземпляров сущностей
            Configuration.ProxyCreationEnabled = false;
        }

        /// <summary>
        /// Обработчик события создания модели БД
        /// </summary>
        /// <param name="modelBuilder"></param>
        protected override void OnModelCreating (DbModelBuilder modelBuilder)
        {
            // Устанавливаем имя схемы
            modelBuilder.HasDefaultSchema( "public" );
            // Ключевым полем делаем EventId
            modelBuilder.Entity<ParameterEvent>().Property( b => b.EventId );

            base.OnModelCreating( modelBuilder );
        }
    }
}
