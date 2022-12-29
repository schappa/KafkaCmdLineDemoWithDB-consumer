using Confluent.Kafka;
using consumer.Models;
using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace consumer
{
    public class DBHelper
    {
        private readonly string connString;

        public DBHelper(string connStr)
        {
            this.connString = connStr;
        }

        public async Task PersistData(ConsumeResult<string, string> cr)
        {
            try
            {
                Shipment shipment = JsonConvert.DeserializeObject<Shipment>(cr.Message.Value);

                using(var connection = new NpgsqlConnection(connString))
                {
                    connection.Open();

                    //Shipment
                    using (var cmd = new NpgsqlCommand("INSERT INTO \"Shipments\" (\"PacketId\", \"PacketRcvd\", \"PacketQty\") " +
                        "VALUES (@p, @q, @r)", connection))
                    {
                        cmd.Parameters.AddWithValue("p", shipment.PacketId.ToString());
                        cmd.Parameters.AddWithValue("q", shipment.PacketRcvd);
                        cmd.Parameters.AddWithValue("r", shipment.PacketQty);
                        await cmd.ExecuteNonQueryAsync();
                    }

                    //Items
                    foreach (var item in shipment.Items)
                    {
                        using (var cmd = new NpgsqlCommand("INSERT INTO \"Items\" " +
                            "(\"PacketId\", \"Name\", \"LotNum\", \"Qty\", \"Dose\", \"DoseMeasurement\") " +
                            "VALUES (@p, @q, @r, @s, @t, @u)", connection))
                        {
                            cmd.Parameters.AddWithValue("p", shipment.PacketId);
                            cmd.Parameters.AddWithValue("q", item.Name);
                            cmd.Parameters.AddWithValue("r", item.LotNum);
                            cmd.Parameters.AddWithValue("s", item.Qty);
                            cmd.Parameters.AddWithValue("t", item.Dose);
                            cmd.Parameters.AddWithValue("u", item.DoseMeasurement);

                            await cmd.ExecuteNonQueryAsync();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }

        }
    }
}
