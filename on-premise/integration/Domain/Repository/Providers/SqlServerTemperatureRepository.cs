using System.Data.SqlClient;
using ProjIntegrador.Domain.Model;
using ProjIntegrador.Domain.Repository.Base;

namespace ProjIntegrador.Domain.Repository.Providers
{
    public class SqlServerTemperatureRepository : ITemperatureRepository
    {
        #region Constants

        private const string CMD_TEXT = "usp_Temperaturas_Upsert";

        #endregion

        #region Attributes

        private readonly string? connString;

        #endregion

        #region Constructors

        internal SqlServerTemperatureRepository()
        {
            connString = Environment.GetEnvironmentVariable("CONN_STRING");
        }

        #endregion

        #region Operations

        public async Task UpsertAsync(Temperature temperature)
        {
            using (SqlConnection sqlConnection = new SqlConnection(connString))
            {
                using (SqlCommand sqlCommand = new SqlCommand())
                {
                    sqlCommand.CommandType = System.Data.CommandType.StoredProcedure;
                    sqlCommand.CommandText = CMD_TEXT;

                    sqlCommand.Parameters.Add(new SqlParameter("@prmRegId", temperature.RegId));
                    sqlCommand.Parameters.Add(new SqlParameter("@prmData", temperature.Data));
                    sqlCommand.Parameters.Add(new SqlParameter("@prmTempMin", temperature.TempMin));
                    sqlCommand.Parameters.Add(new SqlParameter("@prmTempMax", temperature.TempMax));
                    sqlCommand.Parameters.Add(new SqlParameter("@prmTempMed", temperature.TempMed));

                    sqlCommand.Connection = sqlConnection;

                    sqlConnection.Open();

                    await sqlCommand.ExecuteNonQueryAsync();
                }
            }
        }

        #endregion
    }
}
