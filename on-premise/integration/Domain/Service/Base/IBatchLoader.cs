namespace ProjIntegrador.Domain.Service.Base
{
    public interface IBatchLoader
    {
        #region Operations

        Task LoadAsync(CancellationToken cancellationToken);

        #endregion
    }
}
