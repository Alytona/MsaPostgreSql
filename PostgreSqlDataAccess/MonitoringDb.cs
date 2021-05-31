﻿using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations.Schema;

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
        /// Количество записей в таблице событий.
        /// Сделано отдельное свойство, так как Count - метод расширения и в проектах на C++\CLI недоступен 
        /// </summary>

        public int ParameterEventsCount
        {
            get 
            {
                List<string> tableNames = new List<string>();
                var result = Database.SqlQuery( typeof( string ), "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog') AND table_name like 'var_%';" );
                foreach (string tableName in result) {
                    if (tableName.IndexOf( '_', 0 ) != -1 && tableName.IndexOf( '_', 4 ) == -1)
                        tableNames.Add( tableName );
                }
                int count = 0;
                foreach (string tableName in tableNames)
                {
                    var quantityQueryResult = Database.SqlQuery( typeof(int), "select max(event_id) - min(event_id) + 1 from " + tableName + ";" );
                    foreach (int quantity in quantityQueryResult)
                    {
                        count += quantity;
                        // Console.WriteLine( tableName + ": " + quantity );
                    }
                }
                return count;
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
            modelBuilder.HasDefaultSchema( "public" );

            // modelBuilder.Entity<ParameterEvent>().ToTable( "ParameterEvents", "edition_2" );

            // Задаём ключевые поля
            // modelBuilder.Entity<ParameterEvent>().HasKey( b => b.EventId );

            modelBuilder.Entity<Project>().Property( b => b.Id );
            modelBuilder.Entity<Parameter>().Property( b => b.Id );

            base.OnModelCreating( modelBuilder );
        }
    }
}
