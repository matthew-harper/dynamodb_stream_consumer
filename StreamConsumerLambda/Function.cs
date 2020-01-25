using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace StreamConsumerLambda
{
    public class Function
    {
        /// <summary>
        /// A function that consumes a DynamoDB event and prints to CloudWatch Logs
        /// </summary>
        /// <param name="dynamoEvent"></param>
        /// <returns></returns>
        public void FunctionHandler(DynamoDBEvent dynamoEvent, ILambdaContext context)
        {
            context.Logger.LogLine("FunctionHandler invoked");
            foreach (var record in dynamoEvent.Records)
            {
                var primaryKey = record.Dynamodb.Keys;
                var username = primaryKey["Username"].S;

                string oldScore = "0";
                if (record.Dynamodb.OldImage != null)
                {
                    oldScore = record.Dynamodb.OldImage.ContainsKey("TopScore") ?
                                record.Dynamodb.OldImage["TopScore"].N : "0";
                }

                string newScore = record.Dynamodb.NewImage.ContainsKey("TopScore") ?
                                record.Dynamodb.NewImage["TopScore"].N : "0";

                var output = string.Format("{0} increased their high score from {1} to {2}",
                                    username, oldScore, newScore);

                context.Logger.LogLine(output);
            }
        }
    }
}
