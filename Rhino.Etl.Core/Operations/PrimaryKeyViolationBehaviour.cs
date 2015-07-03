using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rhino.Etl.Core.Operations
{
    /// <summary>
    /// The behaviour the engine shoul use for primary key violations
    /// </summary>
    public enum PrimaryKeyViolationBehaviour
    {
        /// <summary>
        /// Ignores primary key violations and continues
        /// </summary>
        Ignore,
        /// <summary>
        /// Standard ADO.NET behaviour, throws SqlException
        /// </summary>
        Throw
    }
}
