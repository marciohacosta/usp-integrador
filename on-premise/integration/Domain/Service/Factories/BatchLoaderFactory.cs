using ProjIntegrador.Domain.Repository.Base;
using ProjIntegrador.Domain.Service.Base;
using ProjIntegrador.Domain.Service.Providers;

namespace ProjIntegrador.Domain.Service.Factories
{
    public class BatchLoaderFactory
    {
        #region Attributes

        private static volatile BatchLoaderFactory instance;

        #endregion

        #region Constructors

        private BatchLoaderFactory()
        {
        }

        #endregion

        #region Properties

        public static BatchLoaderFactory Instance
        {
            get => instance ??= new BatchLoaderFactory();
        }

        #endregion

        #region Operations

        public IBatchLoader Build(ITemperatureRepository temperatureRepository)
        {
            switch (Environment.GetEnvironmentVariable("LOADER_PROVIDER").ToLower())
            {
                case "filesystem":
                    return new FSBatchLoader(temperatureRepository);

                case "mock":
                    return new MockBatchLoader();

                case "sqs":
                    return new SQSBatchLoader(temperatureRepository);

                default:
                    throw new ArgumentException("Invalid loader!");
            }
        }

        #endregion
    }
}
