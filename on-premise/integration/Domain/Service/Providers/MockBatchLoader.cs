using ProjIntegrador.Domain.Service.Base;

namespace ProjIntegrador.Domain.Service.Providers
{
    public class MockBatchLoader : IBatchLoader
    {
        #region Operations

        public async Task LoadAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Run(() =>
                {
                    try
                    {
                        Task.Delay(30000).Wait(cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine("Operation canceled!");
                    }
                    catch (Exception exception)
                    {
                        Console.WriteLine(exception.Message);
                    }
                }, cancellationToken);
            }
        }

        #endregion
    }
}
