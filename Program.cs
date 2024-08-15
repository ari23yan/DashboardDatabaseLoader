using Farapayamak;
using Newtonsoft.Json;
using System;
using System.Data.SqlClient;
using System.Globalization;

namespace MyDatabaseApp
{
    class Program
    {
        static void Main(string[] args)
        {
            // Connection strings
            string connectionString = "Data Source=INFDEV7\\MSSQLSERVER2; Initial Catalog=PelicanManagement;User Id = sa;Password = s@123456;Persist Security Info=True";
            string infdskConnectionString = "Data Source=infdsk1;Initial Catalog=DashboardDatabaseLoaderDB;Persist Security Info=True;TrustServerCertificate=True;User ID=sa;Password=163163163";

            string backupFilePath = @$"D:\DashboardBackup\his_DataBase_backup_{DateTime.Now.ToString("yyyy_MM_dd")}.bak";

            string databaseName = "his_DataBase";
            string status = "Successful";
            string errorMessage = null;
            DateTime startDate = DateTime.Now;
            Console.WriteLine("Hi , Have a good day :)");
            Console.WriteLine(".................");
            Console.WriteLine("Dashboard Database Loader Scheduler Started To Work For Database : " + databaseName);
            Console.WriteLine(".................");
            Console.WriteLine("Started At :" + DateTime.Now);
            Console.WriteLine(".................");
            Console.WriteLine("");
            Console.WriteLine("Please Wait ...");
            while (true)
            {
                if (!File.Exists(backupFilePath))
                {
                    SendSmsToPersonnels(infdskConnectionString, databaseName);
                    status = "Failed";
                    errorMessage = "Backup file not found.";
                    LogRestoreAttempt(infdskConnectionString, backupFilePath, databaseName, status, errorMessage, DateTime.Now - startDate);
                    Console.WriteLine("Error,Backup file not found ...");
                    Thread.Sleep(TimeSpan.FromHours(3));
                    continue;
                }
                string sqlSetSingleUser = $@"ALTER DATABASE [{databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE";
                string sqlRestore = $@"
                            RESTORE DATABASE [{databaseName}]
                            FROM DISK = '{backupFilePath}'
                            WITH REPLACE,
                            MOVE '{databaseName}' TO 'D:\SqlFile\{databaseName}.mdf',
                            MOVE '{databaseName}_log' TO 'D:\SqlFile\{databaseName}_log.ldf'";
                string sqlSetMultiUser = $@"ALTER DATABASE [{databaseName}] SET MULTI_USER";
                try
                {
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();

                        using (SqlCommand command = new SqlCommand(sqlSetSingleUser, connection))
                        {
                            command.CommandTimeout = 600; // Set timeout to 10 minutes
                            command.ExecuteNonQuery();
                        }
                        using (SqlCommand command = new SqlCommand(sqlRestore, connection))
                        {
                            command.CommandTimeout = 3600; // Set timeout to 1 hour
                            command.ExecuteNonQuery(); 
                        }
                        using (SqlCommand command = new SqlCommand(sqlSetMultiUser, connection))
                        {
                            command.CommandTimeout = 600; // Set timeout to 10 minutes
                            command.ExecuteNonQuery();
                        }
                    }
                    LogRestoreAttempt(infdskConnectionString, backupFilePath, databaseName, status, errorMessage, DateTime.Now - startDate);
                    Console.WriteLine("Operation Successful Finished");
                    break;
                }
                catch (Exception ex)
                {
                    status = "Failed";
                    errorMessage = ex.Message;
                    LogRestoreAttempt(infdskConnectionString, backupFilePath, databaseName, status, errorMessage, DateTime.Now - startDate);

                    Thread.Sleep(TimeSpan.FromHours(1));
                }
            }
        }
        static void LogRestoreAttempt(string connectionString, string backupFilePath, string databaseName, string status, string errorMessage, TimeSpan durationTime)
        {
            string sqlInsertLog = @"
                INSERT INTO dbo.RestoreLogs (RestoreDate,RestoreDate_Shamsi, BackupFilePath, Status, ResponseMessage, DatabaseName,Duration)
                VALUES (@RestoreDate,@RestoreDate_Shamsi, @BackupFilePath, @Status, @ResponseMessage, @DatabaseName,@Duration)";
            try
            {
            

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(sqlInsertLog, connection))
                    {
                        command.Parameters.AddWithValue("@RestoreDate", DateTime.Now);
                        command.Parameters.AddWithValue("@RestoreDate_Shamsi", GregorianDateTimeToPersianDate(DateTime.Now));
                        command.Parameters.AddWithValue("@BackupFilePath", backupFilePath);
                        command.Parameters.AddWithValue("@Status", status);
                        command.Parameters.AddWithValue("@ResponseMessage", (object)errorMessage ?? DBNull.Value);
                        command.Parameters.AddWithValue("@DatabaseName", databaseName);
                        command.Parameters.AddWithValue("@Duration",TimeSpan.FromSeconds(Math.Round(durationTime.TotalSeconds)));
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to log the restore attempt: " + ex.Message);
            }
        }
        static void SendSmsToPersonnels(string connectionString,string databaseName)
        {
            string Username = "miladhospital";
            string Password = "S1394#Desk";
            List<string> mobileList = new List<string>();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    string sqlSelectQuery = "SELECT * FROM dbo.Personnel_Notifications WHERE IsActive = 1";

                    using (SqlCommand command = new SqlCommand(sqlSelectQuery, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var personnelMobileNum = reader["Mobile"].ToString().Trim();
                                mobileList.Add(personnelMobileNum);
                            }
                        }
                    }
                }
                if (mobileList.Count <= 0)
                {
                    SmsLogInsert(connectionString, databaseName, false, "Zero Active Mobile Number ", null );
                    return;
                }
                string[] message = new string[mobileList.Count];
                for (int i = 0; i < message.Length; i++)
                {
                    message[i] += " فایل بک آپ ";
                    message[i] += databaseName;
                    message[i] += " در مورخ ";
                    message[i] += GregorianDateToPersianDate(DateTime.Now);
                    message[i] += " کپی نشده است !!! ";
                }
                RestClient restClient = new RestClient(Username, Password);
                restClient.SendMultipleSmartSMS(mobileList.ToArray(), message, "2184090", "10008409084090", "3000700290");
                SmsLogInsert(connectionString, databaseName, true,null, JsonConvert.SerializeObject(mobileList));
            }
            catch (Exception ex)
            {
                SmsLogInsert(connectionString,databaseName,false,ex.Message, JsonConvert.SerializeObject(mobileList));
                Console.WriteLine("Failed to send the sms: " + ex.Message);
            }
        }
        static void SmsLogInsert(string connectionString, string databaseName, bool isSuccessful, string? errorMessage,string? mobiles)
        {
            string sqlInsertLog = @"
                INSERT INTO dbo.SmsSendLog (CreatedDate,CreatedDate_Shamsi, IsSuccessful, DatabaseName, ResponseMessage, Mobiles)
                VALUES (@CreatedDate,@CreatedDate_Shamsi, @IsSuccessful, @DatabaseName, @ResponseMessage, @Mobiles)";
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(sqlInsertLog, connection))
                    {
                        command.Parameters.AddWithValue("@CreatedDate", DateTime.Now);
                        command.Parameters.AddWithValue("@CreatedDate_Shamsi", GregorianDateTimeToPersianDate(DateTime.Now));
                        command.Parameters.AddWithValue("@IsSuccessful", isSuccessful);
                        command.Parameters.AddWithValue("@DatabaseName", databaseName);
                        command.Parameters.AddWithValue("@ResponseMessage", (object)errorMessage ?? DBNull.Value);
                        command.Parameters.AddWithValue("@Mobiles", mobiles);
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to log the sms: " + ex.Message);
            }
        }
        static string GregorianDateTimeToPersianDate(DateTime date)
        {
            PersianCalendar persianCalendar = new PersianCalendar();
            int year = persianCalendar.GetYear(date);
            int month = persianCalendar.GetMonth(date);
            int day = persianCalendar.GetDayOfMonth(date);
            int hour = persianCalendar.GetHour(date);
            int minute = persianCalendar.GetMinute(date);
            int second = persianCalendar.GetSecond(date);
            return $"{year:0000}/{month:00}/{day:00} {hour:00}:{minute:00}:{second:00}";
        }
        static string GregorianDateToPersianDate(DateTime date)
        {
            PersianCalendar persianCalendar = new PersianCalendar();
            int year = persianCalendar.GetYear(date);
            int month = persianCalendar.GetMonth(date);
            int day = persianCalendar.GetDayOfMonth(date);
            return $"{year:0000}/{month:00}/{day:00}";
        }
    }
}
