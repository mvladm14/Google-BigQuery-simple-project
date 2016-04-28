using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Google.Apis.Services;
using System.Threading;
using Google.Apis.Util.Store;
using Google.Apis.Bigquery.v2.Data;

namespace BigQuerySampleProject
{
    public class Program
    {
        // TODO change with the email associated to your google developer account
        private readonly static string email = "your_email";

        private readonly static string query = @"SELECT linja AS bus_line, 
                                                 MAX(ajoaika) AS max_duration,
                                                 MIN(ajoaika)AS min_duration,
                                                 SUM(ajoaika) AS sum_ajoaika,
                                                 AVG(ajoaika) AS median,
                                                 COUNT(DISTINCT(ajoaika)) AS count_distinct
                                                 FROM[HSL_DataSet.hsl_data_table]
                                                 GROUP BY bus_line";

        // TODO change the project id
        // Project ID is in the URL of your project on the APIs Console
        // Project ID looks like "999999";        
        private readonly static string projectId = "your_project_id";

        private readonly static string pathToCredentials = "client_secrets.json";

        public static void Main(string[] args)
        {
            var sample = new Program();

            var service = sample.GetBigqueryService(email).Result;

            var response = sample.ExecuteQuery(service);
            PrintResponse(response);

            Console.WriteLine("\nPress enter to exit");
            Console.ReadLine();
        }
        /// <summary>
        /// Authenticates against Google's cloud platform using the credentials stored
        /// in <see cref="pathToCredentials"/> and email associated to the developer account.  
        /// </summary>
        /// <param name="userEmail">email associated to Google's cloud platform account</param>
        /// <returns>(Asynchronously) the Big Query service</returns>
        private async Task<BigqueryService> GetBigqueryService(string userEmail)
        {
            UserCredential credential;
            using (var stream = new FileStream(pathToCredentials, FileMode.Open, FileAccess.Read))
            {
                credential = await GoogleWebAuthorizationBroker.AuthorizeAsync(
                    GoogleClientSecrets.Load(stream).Secrets,
                    new[]
                    {
                        BigqueryService.Scope.Bigquery
                    },
                    userEmail,
                    CancellationToken.None,
                    new FileDataStore(this.GetType().ToString()));
            }

            return new BigqueryService(new BaseClientService.Initializer()
            {
                HttpClientInitializer = credential,
                ApplicationName = "BigQuerySampleProject",
            });
        }

        private QueryResponse ExecuteQuery(BigqueryService service)
        {
            var jobResource = service.Jobs;
            var qr = new QueryRequest() { Query = query };

            return jobResource.Query(qr, projectId).Execute();
        }

        private static void PrintResponse(QueryResponse response)
        {
            foreach (var row in response.Rows)
            {
                var list = new List<string>();
                foreach (var field in row.F)
                {
                    list.Add(field.V.ToString());
                }
                Console.WriteLine(String.Join("\t", list));
            }
        }
    }
}