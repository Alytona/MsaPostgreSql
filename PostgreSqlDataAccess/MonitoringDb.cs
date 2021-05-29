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
        public DbSet<Project> Projects
        {
            get; set;
        }
        public DbSet<Parameter> Parameters
        {
            get; set;
        }

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
            get 
            {
                ParameterEvent firstEvent = ParameterEvents.OrderBy( record => record.EventId ).FirstOrDefault();
                if (firstEvent == null)
                    return 0;
                return ParameterEvents.Max( e => e.EventId ) - firstEvent.EventId + 1;
            }
        }
        public int? FirstParameterEventId
        {
            get
            {
                return ParameterEvents.OrderBy( record => record.EventId ).FirstOrDefault()?.EventId;
            }
        }
        public int? LastParameterEventId
        {
            get
            {
                return ParameterEvents.OrderByDescending( record => record.EventId ).FirstOrDefault()?.EventId;
            }
        }

        void LogToConsole (string message)
        {
            Console.WriteLine( message );
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="connectionString">Строка с параметрами соединения с сервером БД</param>
        public MonitoringDb (string connectionString) : base( new Npgsql.NpgsqlConnection( connectionString ), contextOwnsConnection: true )
        {
            // this.Database.Log += LogToConsole;

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
            modelBuilder.HasDefaultSchema( "edition_2" );

            // Задаём ключевые поля
            modelBuilder.Entity<ParameterEvent>().Property( b => b.EventId );
            modelBuilder.Entity<Project>().Property( b => b.Id );
            modelBuilder.Entity<Parameter>().Property( b => b.Id );

            base.OnModelCreating( modelBuilder );
        }
    }
}
