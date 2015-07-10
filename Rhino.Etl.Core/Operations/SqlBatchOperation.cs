using System.Configuration;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Core.Operations
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;

    /// <summary>
    /// Perform a batch command against SQL server
    /// </summary>
    public abstract class SqlBatchOperation : AbstractDatabaseOperation
    {
        /// <summary>
        /// Gets or sets the size of the batch.
        /// </summary>
        /// <value>The size of the batch.</value>
        public int BatchSize { get; set; } = 50;

        /// <summary>
        /// The timeout of the command set
        /// </summary>
        public int Timeout { get; set; } = 30;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlBatchOperation"/> class.
        /// </summary>
        /// <param name="connectionStringName">Name of the connection string.</param>
        public SqlBatchOperation(string connectionStringName)
            : this(ConfigurationManager.ConnectionStrings[connectionStringName])
        {            
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlBatchOperation"/> class.
        /// </summary>
        /// <param name="connectionStringSettings">The connection string settings to use.</param>
        public SqlBatchOperation(ConnectionStringSettings connectionStringSettings)
            : base(connectionStringSettings)
        {
            base.paramPrefix = "@";
        }

        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <returns></returns>
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            Guard.Against<ArgumentException>(rows == null, "SqlBatchOperation cannot accept a null enumerator");
            using (var connection = (SqlConnection)Use.Connection(ConnectionStringSettings))
            using (var transaction = (SqlTransaction) BeginTransaction(connection))
            {
                SqlCommandSet commandSet = null;
                CreateCommandSet(connection, transaction, ref commandSet, Timeout);
                foreach (var row in rows)
                {
                    var command = new SqlCommand();
                    PrepareCommand(row, command);
                    if (command.Parameters.Count == 0) //workaround around a framework bug
                    {
                        var guid = Guid.NewGuid();
                        command.Parameters.AddWithValue(guid.ToString(), guid);
                    }
                    commandSet.Append(command);
                    if (commandSet.CountOfCommands >= BatchSize)
                    {
                        Statistics.AddOutputRows(commandSet.ExecuteNonQuery());

                        CreateCommandSet(connection, transaction, ref commandSet, Timeout);
                    }
                }
                Statistics.AddOutputRows(commandSet.ExecuteNonQuery());

                if (PipelineExecuter.HasErrors)
                {
                    Warn(null, "Rolling back transaction in {0}", Name);
                    if (transaction != null) transaction.Rollback();
                    Warn(null, "Rolled back transaction in {0}", Name);
                }
                else
                {
                    Debug("Committing {0}", Name);
                    if (transaction != null) transaction.Commit();
                    Debug("Committed {0}", Name);
                }                    

            }
            yield break;
        }

        /// <summary>
        /// Prepares the command from the given row
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="command">The command.</param>
        protected abstract void PrepareCommand(Row row, SqlCommand command);

        private static void CreateCommandSet(SqlConnection connection, SqlTransaction transaction, ref SqlCommandSet commandSet, int timeout)
        {
            commandSet?.Dispose();
            commandSet = new SqlCommandSet
            {
                Connection = connection, 
                Transaction = transaction,
                CommandTimeout = timeout,
            };
        }
    }
}