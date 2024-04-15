using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using ProjIntegrador.Domain.Model;
using ProjIntegrador.Domain.Repository.Base;
using ProjIntegrador.Domain.Service.Base;
using System.Text.Json;

namespace ProjIntegrador.Domain.Service.Providers
{
    public class SQSBatchLoader : IBatchLoader
    {
        #region Attributes

        private readonly ITemperatureRepository temperatureRepository;
        private readonly IAmazonSQS sqsClient;
        private readonly IAmazonS3 s3Client;
        private readonly string queueUrl;
        private readonly int maxNumberOfMessages;
        private readonly int waitTimeSeconds;
        private readonly string bucketName;
        private readonly string jsonPath;

        #endregion

        #region Constructors

        internal SQSBatchLoader(ITemperatureRepository temperatureRepository)
        {
            this.temperatureRepository = temperatureRepository;

            BasicAWSCredentials awsCredentials = new BasicAWSCredentials(Environment.GetEnvironmentVariable("AWS_ACCESS_KEY"), Environment.GetEnvironmentVariable("AWS_SECRET_KEY"));
            RegionEndpoint regionEndpoint      = RegionEndpoint.GetBySystemName(Environment.GetEnvironmentVariable("AWS_REGION"));

            sqsClient           = new AmazonSQSClient(awsCredentials, regionEndpoint);
            queueUrl            = Environment.GetEnvironmentVariable("AWS_REFINED_SQS_URL");
            maxNumberOfMessages = int.Parse(Environment.GetEnvironmentVariable("AWS_SQS_MAX_MESSAGES"));
            waitTimeSeconds     = int.Parse(Environment.GetEnvironmentVariable("AWS_SQS_MESSAGE_WAIT"));

            s3Client            = new AmazonS3Client(awsCredentials, regionEndpoint);
            bucketName          = Environment.GetEnvironmentVariable("AWS_REFINED_S3");
            jsonPath            = Environment.GetEnvironmentVariable("JSON_PATH");
        }

        #endregion

        #region Operations

        public async Task LoadAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                    {
                        MaxNumberOfMessages = maxNumberOfMessages,
                        QueueUrl            = queueUrl,
                        WaitTimeSeconds     = waitTimeSeconds
                    };

                    ReceiveMessageResponse receiveMessageResponse = await sqsClient.ReceiveMessageAsync(receiveMessageRequest, cancellationToken);

                    if (receiveMessageResponse.Messages.Count > 0)
                    {
                        for (int i = 0; i < receiveMessageResponse.Messages.Count; i++)
                        {
                            string messageBody = receiveMessageResponse.Messages[i].Body;
                            string jsonFile    = $"{messageBody}.json";

                            Console.WriteLine($"{DateTime.Now:O} - Processing {jsonFile}");

                            // Load from S3
                            GetObjectRequest getObjectRequest = new GetObjectRequest()
                            {
                                BucketName = bucketName,
                                Key        = jsonFile
                            };

                            // Save json local file
                            string fullPath = Path.Combine(jsonPath, jsonFile);

                            using (GetObjectResponse getObjectResponse = await s3Client.GetObjectAsync(getObjectRequest, cancellationToken))
                            {
                                await getObjectResponse.WriteResponseStreamToFileAsync(fullPath, false, cancellationToken);
                            }

                            // Upsert temperature
                            Temperature? temperature;

                            JsonSerializerOptions options = new JsonSerializerOptions()
                            {
                                PropertyNameCaseInsensitive = true
                            };

                            using (StreamReader reader = new StreamReader(fullPath))
                            {
                                string json = await reader.ReadToEndAsync(cancellationToken);
                                temperature = JsonSerializer.Deserialize<Temperature>(json, options);
                            }

                            await temperatureRepository.UpsertAsync(temperature);

                            // Delete message
                            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest()
                            {
                                QueueUrl      = queueUrl,
                                ReceiptHandle = receiveMessageResponse.Messages[i].ReceiptHandle
                            };

                            await sqsClient.DeleteMessageAsync(deleteMessageRequest, cancellationToken);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine( "Operation canceled!");
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception.Message);
                }

                await Task.Delay(1000, cancellationToken);
            }
        }

        #endregion
    }
}
