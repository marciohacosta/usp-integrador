using ProjIntegrador.Domain.Repository.Base;
using ProjIntegrador.Domain.Repository.Providers;

namespace ProjIntegrador.Domain.Repository.Factories
{
    public class TemperatureRepositoryFactory
    {
        #region Attributes

        private static volatile TemperatureRepositoryFactory instance;

        #endregion

        #region Constructors

        private TemperatureRepositoryFactory()
        {
        }

        #endregion

        #region Properties

        public static TemperatureRepositoryFactory Instance
        {
            get => instance ??= new TemperatureRepositoryFactory();
        }

        #endregion

        #region Operations

        public ITemperatureRepository Build()
        {
            switch (Environment.GetEnvironmentVariable("REPOSITORY_PROVIDER").ToLower())
            {
                case "mock":
                    return new MockTemperatureRepository();

                case "sqlserver":
                    return new SqlServerTemperatureRepository();

                default:
                    throw new ArgumentException("Invalid temperature repoitory!");
            }
        }

        #endregion
    }
}
