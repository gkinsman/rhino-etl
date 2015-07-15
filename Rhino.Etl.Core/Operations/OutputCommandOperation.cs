using System.Configuration;
using System.Data.SqlClient;
using Rhino.Etl.Core.Enumerables;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Core.Operations
{
    using System.Collections.Generic;
    using System.Data;

    /// <summary>
    /// Generic output command operation
    /// </summary>
    public abstract class OutputCommandOperation : AbstractCommandOperation
    {
        private const int PrimaryKeyViolationErrorCode = 2627;

        /// <summary>
        /// Initializes a new instance of the <see cref="OutputCommandOperation"/> class.
        /// </summary>
        /// <param name="connectionStringName">Name of the connection string.</param>
        public OutputCommandOperation(string connectionStringName) : this(ConfigurationManager.ConnectionStrings[connectionStringName])
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OutputCommandOperation"/> class.
        /// </summary>
        /// <param name="connectionStringSettings">Connection string settings to use.</param>
        public OutputCommandOperation(ConnectionStringSettings connectionStringSettings)
            : base(connectionStringSettings)
        {
            PrimaryKeyViolationBehaviour = PrimaryKeyViolationBehaviour.Throw;
        }

        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <returns></returns>
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            using (IDbConnection connection = Use.Connection(ConnectionStringSettings))
            using (IDbTransaction transaction = BeginTransaction(connection))
            {
                foreach (Row row in new SingleRowEventRaisingEnumerator(this, rows))
                {
                    using (IDbCommand cmd = connection.CreateCommand())
                    {
                        cmd.CommandTimeout = 60*60;
                        currentCommand = cmd;
                        currentCommand.Transaction = transaction;
                        PrepareCommand(currentCommand, row);

                        if (PrimaryKeyViolationBehaviour == PrimaryKeyViolationBehaviour.Ignore)
                        {
                            try
                            {
                                currentCommand.ExecuteNonQuery();
                            }
                            catch (SqlException ex)
                            {
                                if (ex.Number == PrimaryKeyViolationErrorCode)
                                {
                                    Trace("Ignoring PRIMARY KEY violation");
                                }
                                else throw;
                            }
                        }
                        else
                        {
                            currentCommand.ExecuteNonQuery();
                        }
                    }
                }
                if (PipelineExecuter.HasErrors)
                {
                    Warn("Rolling back transaction in {0}", Name);
                    transaction.Rollback();
                    Warn("Rolled back transaction in {0}", Name);
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
        /// Prepares the command for execution, set command text, parameters, etc
        /// </summary>
        /// <param name="cmd">The command.</param>
        /// <param name="row">The row.</param>
        protected abstract void PrepareCommand(IDbCommand cmd, Row row);


        /// <summary>
        /// Gets or sets the primary key behaviour
        /// </summary>
        public PrimaryKeyViolationBehaviour PrimaryKeyViolationBehaviour { get; set; }
    }
}
