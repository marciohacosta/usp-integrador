using ProjIntegrador.Domain.Model;

namespace ProjIntegrador.Domain.Repository.Base
{
    public interface ITemperatureRepository
    {
        #region Operations

        Task UpsertAsync(Temperature temperature);

        #endregion
    }
}
