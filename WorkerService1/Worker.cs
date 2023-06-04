using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using System.Xml.Linq;
using Newtonsoft.Json;

/*
Sean Fite
Cloud Commputing 
Last Updated 5/31/23
This Worker Service implements long pulling to recieve SQS queue messages. Comparing with an insurance database for validity of 
insurance policy. The results are sent back to the SQS queue.
*/

namespace WorkerService1
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;                 
        public string customerID;
        private const string loggerPath = "C:\\Users\\squir\\OneDrive\\Desktop\\Project2\\Temp.txt";
        private string recieptHandler;
        bool hasNewMessage = false;
        List<string> patientIds = new List<string>();

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            await base.StartAsync(cancellationToken);
            string xmlFilePath = "C:\\Users\\squir\\OneDrive\\Desktop\\InsuranceDatabase.xml";
            string jsonFilePath = "C:\\Users\\squir\\OneDrive\\Desktop\\Patient.json";
            string jsonContent = "";
            string messageGroupID = "";
            string returnMessage = "Insurance ID Doesn't Match";
            string myAccessKey = "ASIA56JY2SKBZZHAKK3D";
            string mySecretKey = "cPVeZsi9bSfOUCjU4wYY5oSa0SsoNxnGNiVYRS2u";
            string sessionToken = "FwoGZXIvYXdzEOL//////////wEaDD8ezOp8FOqqJMPXtCLOATNpQ4adGFKLnHJ2B2c9gsJSMX3S0O+/4m1vZXPNsAdNW7R" +
                "ZJE9DZAtAp/WIZabkg3k6XI/Opf5D/qxGMurex9lgEl2S7f3hMaJET6TWylLkkrlCplklr0VtYnRTEaZyP1eD/LQRabDPMTzh5kyWVUUtHWsxo7Mn" +
                "i2qtKDDiGz9t3EKbqVN+I1AnNRSzhrYtnrhNsN7m8yfIrDEoV2d+jIQUJH85KZoHcxnT4ZMULWVRO2t/5MqfUpvenBjcwOMa2FqN5wiPSO8qkBrZnR+" +
                "JKIaD86MGMi3hQ8rAYUTxpHr8ZeiE9O8ReTM01VsIbpy+kO52YLTTn30A6kBWeHHmwMEsXWg=";
            // get AWS credentials
            SessionAWSCredentials credentials = new SessionAWSCredentials(myAccessKey, mySecretKey, sessionToken);      
            AmazonSQSConfig sqsConfig = new AmazonSQSConfig
            {
                RegionEndpoint = Amazon.RegionEndpoint.USEast1                      
            };
            // initialize object of SQS 
            AmazonSQSClient sQSClient = new AmazonSQSClient(credentials, sqsConfig);                       
            while (!cancellationToken.IsCancellationRequested)
            {
                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest
                {
                    QueueUrl = "https://sqs.us-east-1.amazonaws.com/958433759875/tester",                    
                    MaxNumberOfMessages = 1,
                    // wait time set to enable long polling
                    WaitTimeSeconds = 20                                                        
                };
                // recieve messages from sqs queue
                ReceiveMessageResponse receiveMessageResponse = await sQSClient.ReceiveMessageAsync(receiveMessageRequest, cancellationToken);
                if (receiveMessageResponse.Messages.Count > 0)
                {                                                                           
                    foreach (var message in receiveMessageResponse.Messages)
                    {
                        recieptHandler = message.ReceiptHandle;
                        string messageBody = message.Body;
                        WriteLog("Read message: " + messageBody);
                        // Deserialize the JSON message body
                        var json = JsonConvert.DeserializeObject<dynamic>(messageBody);
                        // Extract the "id" value from the JSON
                        string searchPatientId = json.id;
                        // Parse the XML file
                        XDocument document = XDocument.Load(xmlFilePath);
                        XElement rootElement = document.Root;
                        if (rootElement != null)
                        {
                            // set up json vs xml matching variable
                            var matchingPatientElement = rootElement.Elements("patient")
                                .FirstOrDefault(e => (string)e.Attribute("id") == searchPatientId);
                            // if there is a match (SQS queue message data matches insurance database data)
                            if (matchingPatientElement != null)
                            {
                                XElement policyElement = matchingPatientElement.Element("policy");
                                if (policyElement != null)
                                {
                                    XAttribute policyNumberAttribute = policyElement.Attribute("policyNumber");
                                    XElement providerElement = policyElement.Element("provider");
                                    if (policyNumberAttribute != null && providerElement != null)
                                    {
                                        string policyNumber = policyNumberAttribute.Value;
                                        string provider = providerElement.Value;
                                        var patientData = new
                                        {
                                            PatientId = searchPatientId,
                                            PolicyNumber = policyNumber,
                                            Provider = provider
                                        };
                                        // convert parsed objects into json file
                                        jsonContent = JsonConvert.SerializeObject(patientData, Newtonsoft.Json.Formatting.Indented);                                                                       
                                        System.IO.File.WriteAllText(jsonFilePath, jsonContent);
                                    }
                                }
                            }
                            else
                            {
                                // if patient profile not found in database
                                var notFoundData = new
                                {
                                    Message = "Patient Profile Not Found"
                                };
                                jsonContent = JsonConvert.SerializeObject(notFoundData, Newtonsoft.Json.Formatting.Indented);
                                System.IO.File.WriteAllText(jsonFilePath, jsonContent);
                            }                     
                        }
                    }
                    // Send the response message back to the SQS queue
                    string backToQueue = System.IO.File.ReadAllText(jsonFilePath);
                    SendMessageRequest sendMessageRequest = new SendMessageRequest
                    {
     
                        QueueUrl = "https://sqs.us-east-1.amazonaws.com/958433759875/Regular",
                        MessageBody = backToQueue,
                    };               
                    await sQSClient.SendMessageAsync(sendMessageRequest, cancellationToken);
                    WriteLog("Posted Message: " + jsonContent);
                    // delete message received from queue after read
                    DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest
                    {
                        QueueUrl = "https://sqs.us-east-1.amazonaws.com/958433759875/tester",
                        ReceiptHandle = recieptHandler
                    };
                    await sQSClient.DeleteMessageAsync(deleteMessageRequest, cancellationToken);
                }
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // what should we do when it stops
                await base.StopAsync(cancellationToken);
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }

        // logging worker service actions
        public void WriteLog(string message)
        {
            string text = string.Format("{0}:\t{1}", DateTime.Now, message, Environment.NewLine);
            using (StreamWriter writer = new StreamWriter(loggerPath, append: true))
            {
                writer.WriteLine(text);
            }
        }     
    }   
}