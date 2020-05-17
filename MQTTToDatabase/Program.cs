using Newtonsoft.Json;
using System;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace MQTTToDatabase
{
    class Program
    {

        MqttClient client;
        Database database;

        static void Main(string[] args)
        {
            Program program = new Program();
            program.Run();

            Console.ReadLine();
        }


        public void Run()
        {
            database = new Database();

            while(true)
            {
                try
                {
                    if (client == null)
                    {
                        client = new MqttClient("test.mosquitto.org"); //Of je kan hier ook jouw eigen broker gebruiken uit vorig labo.
                        client.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;
                        client.Connect(new Guid().ToString());
                        
                        if (client.IsConnected)
                        {
                            client.Subscribe(new string[] { "hogent/elm/test" }, new byte[] { MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE });
                        }
                    }
                }
                catch (Exception)
                {
                }

                Thread.Sleep(1000);
            }
            

        }

        private void Client_MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
        {
            string json = Encoding.UTF8.GetString(e.Message);
            Console.WriteLine(json);

            SensorMeasurement sensorMeasurement = JsonConvert.DeserializeObject<SensorMeasurement>(json);

            //Write to DB.
            database.Insert(sensorMeasurement);
        }
    }

    class SensorMeasurement
    {
        public int IDSensor { get; set; }
        public DateTime Datetime { get; set; }
        public double Value { get; set; }
    }

    class Database
    {

        SqlConnection sqlConnection;
        public Database()
        {
            sqlConnection = new SqlConnection("Data Source=192.168.1.39;Initial Catalog=SensorMeasurements;User ID=sa;Password=P@ssw0rd;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False");

        }

        public void Insert(SensorMeasurement sensorMeasurement)
        {
            sqlConnection.Open();

            SqlCommand sqlCommand = sqlConnection.CreateCommand();

            sqlCommand.CommandText = "INSERT INTO Measurement (IDSensor, Datetime, Value) VALUES (@IDSensor, @Datetime, @Value)";

            sqlCommand.Parameters.Add("@IDSensor", System.Data.SqlDbType.Int).Value = sensorMeasurement.IDSensor;
            sqlCommand.Parameters.Add("@Datetime", System.Data.SqlDbType.DateTime2).Value = sensorMeasurement.Datetime;
            sqlCommand.Parameters.Add("@Value", System.Data.SqlDbType.Float).Value = sensorMeasurement.Value;

            sqlCommand.ExecuteNonQuery();

            sqlConnection.Close();
        }
    }
}
