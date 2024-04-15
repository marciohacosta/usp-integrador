using System.Collections.Concurrent;
using System.Text.Json;
using ProjIntegrador.Domain.Model;
using ProjIntegrador.Domain.Repository.Base;
using ProjIntegrador.Domain.Service.Base;

namespace ProjIntegrador.Domain.Service.Providers
{
    public class FSBatchLoader : IBatchLoader
    {
        #region Attributes

        private readonly ITemperatureRepository temperatureRepository;

        #endregion

        #region Constructors

        internal FSBatchLoader(ITemperatureRepository temperatureRepository)
        {
            this.temperatureRepository = temperatureRepository;
        }

        #endregion

        #region Operations

        public async Task LoadAsync(CancellationToken cancellationToken)
        {
            try
            {
                string? jsonPath = Environment.GetEnvironmentVariable("JSON_PATH");

                IEnumerable<string> jsonFiles = Directory.GetFiles(jsonPath);

                JsonSerializerOptions options = new JsonSerializerOptions()
                {
                    PropertyNameCaseInsensitive = true
                };

                ConcurrentBag<string> errors = new ConcurrentBag<string>();

                await Parallel.ForEachAsync(jsonFiles, async (jsonFile, CancellationToken) =>
                {
                    Temperature? temperature;

                    using (StreamReader reader = new StreamReader(jsonFile))
                    {
                        string json = await reader.ReadToEndAsync(cancellationToken);
                        temperature = JsonSerializer.Deserialize<Temperature>(json, options);
                    }

                    if (temperature == null)
                    {
                        errors.Add(jsonFile);
                    }
                    else
                    {
                        await temperatureRepository.UpsertAsync(temperature);
                    }
                });

                if (!errors.IsEmpty)
                {
                    Console.WriteLine($"Batch processed with errors: {string.Join(",", errors.ToArray())}");
                }
                else
                {
                    Console.WriteLine("Batch load completed successfully!");
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Operation canceled!");
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception.Message);
            }
        }

        #endregion
    }
}
