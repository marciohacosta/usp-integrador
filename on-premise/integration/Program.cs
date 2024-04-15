using ProjIntegrador.Domain.Repository.Base;
using ProjIntegrador.Domain.Repository.Factories;
using ProjIntegrador.Domain.Service.Base;
using ProjIntegrador.Domain.Service.Factories;

namespace ProjIntegrador
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Loading refined data...");

            CancellationTokenSource cts = new CancellationTokenSource();

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            ITemperatureRepository temperatureRepository = TemperatureRepositoryFactory.Instance.Build();
            IBatchLoader batchLoader                     = BatchLoaderFactory.Instance.Build(temperatureRepository);

            await batchLoader.LoadAsync(cts.Token);
        }
    }
}
