using ProjIntegrador.Domain.Model;
using ProjIntegrador.Domain.Repository.Base;

namespace ProjIntegrador.Domain.Repository.Providers
{
    public class MockTemperatureRepository : ITemperatureRepository
    {
        #region Operations

        public async Task UpsertAsync(Temperature temperature)
        {
            await Task.Run(() =>
            {
                Task.Delay(1000);
            });
        }

        #endregion
    }
}
