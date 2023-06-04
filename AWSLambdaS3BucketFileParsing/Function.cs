using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using System.Xml;
using Amazon.SQS;
using Amazon.SQS.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AWSLambdaS3BucketFileParsing;

public class Function
{
    public static string siteId = "";
    IAmazonS3 S3Client { get; set; }
    IAmazonSQS SQSClient { get; set; }

    public Function()
    {
        S3Client = new AmazonS3Client();
        SQSClient = new AmazonSQSClient();
    }

    public Function(IAmazonS3 s3Client, IAmazonSQS sQSClient) 
    {
        this.S3Client = s3Client;
        this.SQSClient = sQSClient;  
    }

    public async Task FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        var jsonContent = "";
        // object for s3 bucket events
        var eventRecords = evnt.Records ?? new List<S3Event.S3EventNotificationRecord>();
        // for each event
        foreach (var record in eventRecords)
        {
            var s3Event = record.S3;
            // if there is an event and it involves bucket projet-test-02
            if (s3Event == null || s3Event.Bucket.Name != "project-test-02") 
            {
                continue;
            }

            try
            {
                // when s3 bucket recieves file upload
                var response = await this.S3Client.GetObjectAsync(s3Event.Bucket.Name, s3Event.Object.Key);
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    // read XML file
                    var xmlContent = reader.ReadToEnd();
                    var xmlDoc = new XmlDocument();
                    xmlDoc.LoadXml(xmlContent);
                    // Convert XML to JSON using Newtonsoft.Json
                    jsonContent = Newtonsoft.Json.JsonConvert.SerializeXmlNode(xmlDoc, Newtonsoft.Json.Formatting.None, true);
                    Console.WriteLine(siteId);
                }
            }
            catch (Exception e)
            {
                context.Logger.LogError($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogError(e.Message);
                context.Logger.LogError(e.StackTrace);
                throw;
            }
            // Pass the value to the existing message queue
            var sendMessageRequest = new SendMessageRequest
            {
                QueueUrl = "https://sqs.us-east-1.amazonaws.com/958433759875/tester",
                MessageBody = jsonContent,
            };
            try
            {
                var response = await SQSClient.SendMessageAsync(sendMessageRequest);
                Console.WriteLine($"Message sent successfully. Message ID: {response.MessageId}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error sending message to queue. {e.Message}");
            }
        }
    }
}
