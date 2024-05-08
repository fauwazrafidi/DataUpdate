using System.Data;
using FirebirdSql.Data.FirebirdClient;
using Npgsql;

namespace DataUpdate
{
    public class FdbToPostgresUpdater
    {
        private readonly string _fdbConnectionString;
        private readonly string _postgresConnectionString;
        private readonly string _fdbTable1Name;
        private readonly string _fdbTable2Name;
        private readonly string _commonColumn;
        private readonly string _postgresTableName;
        private readonly List<string> _fdbColumnsToRetrieve1;
        private readonly List<string> _fdbColumnsToRetrieve2;


        public FdbToPostgresUpdater(string fdbConnectionString, string postgresConnectionString, string fdbName1, string fdbName2, string commonColumn, string postgresTableName, List<string> fdbColumns1, List<string> fdbColumns2)
        {
            _fdbConnectionString = fdbConnectionString;
            _postgresConnectionString = postgresConnectionString;
            _fdbTable1Name = fdbName1;
            _fdbTable2Name = fdbName2;
            _postgresTableName = postgresTableName;
            _commonColumn = commonColumn;
            _fdbColumnsToRetrieve1 = fdbColumns1;
            _fdbColumnsToRetrieve2 = fdbColumns2;
        }

        public void UpdateData()
        {
            // Read data from FDB file
            DataTable combinedData = GetCombinedFdbData();

            // Update data in PostgreSQL
            UpdatePostgresTable(combinedData);
        }

        private DataTable GetCombinedFdbData()
        {
            DataTable dataTable = new DataTable();

            using (FbConnection connection = new FbConnection(_fdbConnectionString))
            {
                connection.Open();

                // Build a JOIN query using the common column and selecting columns from both tables
                string joinQuery = $"SELECT " +
                   $"{string.Join(",", _fdbColumnsToRetrieve1.Select(c => $"t1.{c} AS {c}1"))}, " +
                   $"{string.Join(",", _fdbColumnsToRetrieve2)} " + // No alias for DOCDATE
                   $"FROM {_fdbTable1Name} AS t1 " +
                   $"INNER JOIN {_fdbTable2Name} AS t2 ON t1.{_commonColumn} = t2.{_commonColumn}";



                using (FbDataAdapter adapter = new FbDataAdapter(joinQuery, connection))
                {
                    Console.WriteLine(joinQuery);
                    adapter.Fill(dataTable);
                }
            }

            return dataTable;
        }

        private void UpdatePostgresTable(DataTable data)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(_postgresConnectionString))
            {
                connection.Open();

                // Define the unique identifier column name from FDB
                string uniqueIdentifierColumn = "DTLKEY1"; // Replace with your actual column name
                string qtyColumn = "QTY1";
                string qtyRemainColumn = "qtyremain";

                foreach (DataRow row in data.Rows)
                {
                    // Get the unique identifier value from the current row
                    string uniqueIdentifier = row[uniqueIdentifierColumn].ToString();
                    decimal qtyValue = Convert.ToDecimal(row[qtyColumn]);

                    // Check if a record with the same identifier exists in PostgreSQL
                    string selectQuery = $"SELECT COUNT(*) FROM {_postgresTableName} WHERE {uniqueIdentifierColumn} = " + uniqueIdentifier;
                    using (NpgsqlCommand selectCommand = new NpgsqlCommand(selectQuery, connection))
                    {
                        selectCommand.Parameters.AddWithValue("@uniqueIdentifier", uniqueIdentifier);
                        int recordCount = Convert.ToInt32(selectCommand.ExecuteScalar());
                        bool recordExists = recordCount > 0;

                        if (recordExists)
                        {
                            // Update existing record
                            string updateColumns = string.Join(",", Enumerable.Range(0, data.Columns.Count).Select(i => $"{data.Columns[i].ColumnName} = @{i}"));
                            // Include update for qtyremain if it's not already set
                            updateColumns += $", {qtyRemainColumn} = COALESCE({qtyRemainColumn}, @{data.Columns.Count})";
                            string updateQuery = $"UPDATE {_postgresTableName} SET {updateColumns} WHERE {uniqueIdentifierColumn} = " + uniqueIdentifier;

                            using (NpgsqlCommand updateCommand = new NpgsqlCommand(updateQuery, connection))
                            {
                                for (int i = 0; i < data.Columns.Count; i++)
                                {
                                    updateCommand.Parameters.AddWithValue($"@{i}", row[i]);
                                }
                                updateCommand.Parameters.AddWithValue($"@{data.Columns.Count}", qtyValue);
                                updateCommand.Parameters.AddWithValue("@uniqueIdentifier", uniqueIdentifier);
                                updateCommand.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            // Insert new record
                            string columnNames = string.Join(",", Enumerable.Range(0, data.Columns.Count).Select(i => $"{data.Columns[i].ColumnName}"));
                            string placeholders = string.Join(",", Enumerable.Range(0, data.Columns.Count).Select(i => $"@{i}"));
                            // Include qtyremain in column names and placeholders
                            columnNames += $", {qtyRemainColumn}";
                            placeholders += $", @{data.Columns.Count}";

                            string insertQuery = $"INSERT INTO {_postgresTableName} ({columnNames}) VALUES ({placeholders})";

                            using (NpgsqlCommand insertCommand = new NpgsqlCommand(insertQuery, connection))
                            {
                                for (int i = 0; i < data.Columns.Count; i++)
                                {
                                    insertCommand.Parameters.AddWithValue($"@{i}", row[i]);
                                }
                                insertCommand.Parameters.AddWithValue($"@{data.Columns.Count}", qtyValue);
                                insertCommand.ExecuteNonQuery();
                            }
                        }
                    }
                }
            }
        }


        static void Main(string[] args)
        {
            // Replace connection string and table names with your actual values
            string fdbConnectionString = "database=C:\\Users\\User\\Desktop\\Polynic\\ACC-0004.FDB;user=SYSDBA;password=masterkey;DataSource=localhost;Port=3050;Dialect=3;Charset=UTF8;\"";
            string postgresConnectionString = "Host=localhost;Database=postgres;Username=postgres;Password=1234";
            string fdbTable1Name = "PH_PIDTL";
            string fdbTable2Name = "PH_PI";
            string postgresTableName = "Label_Data";
            string commonColumn = "DOCKEY";

            // Specify the list of columns to retrieve from the FDB table
            List<string> fdbColumns1ToRetrieve = new List<string>() { "REMARK2", "ITEMCODE", "DESCRIPTION", "DESCRIPTION2", "BATCH", "LOCATION", "QTY", "UOM", "DTLKEY" }; // Replace with your desired columns
            List<string> fdbColumns2ToRetrieve = new List<string>() { "DOCDATE"};

            FdbToPostgresUpdater updater = new FdbToPostgresUpdater(fdbConnectionString, postgresConnectionString, fdbTable1Name, fdbTable2Name, commonColumn, postgresTableName, fdbColumns1ToRetrieve, fdbColumns2ToRetrieve);
            updater.UpdateData();

            Console.WriteLine("Data update completed.");

        }
    }
}
