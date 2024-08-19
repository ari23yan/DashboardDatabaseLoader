using Farapayamak;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Data.SqlClient;
using System.Globalization;

namespace MyDatabaseApp
{
    class Program
    {
        static void Main(string[] args)
        {
            if (IsHoliday(DateTime.Now).Result) /// برای روزهای تعطیل دیتابیس لود نمیشود
            {
                return;
            }
            string infdskConnectionString = "Data Source=infdsk1;Initial Catalog=DashboardDatabaseLoaderDB;Persist Security Info=True;TrustServerCertificate=True;User ID=sa;Password=163163163";
            string backupFilePath = @$"D:\HisBackup\his_DataBase_backup_{DateTime.Now.ToString("yyyy_MM_dd")}.bak";
            string databaseName = "his_DataBase";
            string status = "OnProgress";
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
            var logRestorID = InsertLogRestoreAttempt(infdskConnectionString, backupFilePath, databaseName, status, errorMessage, DateTime.Now - startDate);
            while (true)
            {
                if (!File.Exists(backupFilePath))
                {
                    SendSmsToPersonnels(infdskConnectionString, databaseName);
                    status = "Failed";
                    errorMessage = "Backup file not found.";
                    UpdateLogRestoreAttempt(infdskConnectionString, logRestorID, backupFilePath, databaseName, status, errorMessage, DateTime.Now - startDate);
                    Console.WriteLine("Error,Backup file not found ...");
                    Console.WriteLine("Program Start Work At  -> " + DateTime.Now.AddHours(+3));
                    Thread.Sleep(TimeSpan.FromHours(3));
                    continue;
                }
                string sqlSetSingleUser = $@"ALTER DATABASE [{databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE";
                string sqlRestore = $@"
                            RESTORE DATABASE [{databaseName}]
                            FROM DISK = '{backupFilePath}'
                            WITH REPLACE,
                            MOVE '{databaseName}' TO 'H:\data ware1\{databaseName}.mdf',
                            MOVE '{databaseName}_log' TO 'H:\data ware1\{databaseName}_log.ldf'";
                string sqlSetMultiUser = $@"ALTER DATABASE [{databaseName}] SET MULTI_USER";
                try
                {
                    using (SqlConnection connection = new SqlConnection(infdskConnectionString))
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
                    status = "Successful";
                    errorMessage = "";
                    UpdateLogRestoreAttempt(infdskConnectionString, logRestorID, backupFilePath, databaseName, status, errorMessage, DateTime.Now - startDate);
                    Console.WriteLine("Operation Successful Finished");
                    Console.ReadKey();
                    break;
                }
                catch (Exception ex)
                {
                    status = "Failed,Exception";
                    errorMessage = ex.Message;
                    UpdateLogRestoreAttempt(infdskConnectionString, logRestorID, backupFilePath, databaseName, status, errorMessage, DateTime.Now - startDate);
                }
            }
        }
        static long InsertLogRestoreAttempt(string connectionString, string backupFilePath, string databaseName, string status, string errorMessage, TimeSpan durationTime)
        {
            long restoreId = 0;

            string sqlInsertLog = @"
        INSERT INTO dbo.RestoreLogs (RestoreDate, RestoreDate_Shamsi, BackupFilePath, Status, ResponseMessage, DatabaseName, Duration)
        VALUES (@RestoreDate, @RestoreDate_Shamsi, @BackupFilePath, @Status, @ResponseMessage, @DatabaseName, @Duration);
        SELECT CAST(SCOPE_IDENTITY() AS int);";

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
                        command.Parameters.AddWithValue("@Duration", TimeSpan.FromSeconds(Math.Round(durationTime.TotalSeconds)));
                        restoreId = Convert.ToInt64(command.ExecuteScalar());
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to log the restore attempt: " + ex.Message);
            }

            return restoreId;
        }
        static void UpdateLogRestoreAttempt(string connectionString, long restoreId, string backupFilePath, string databaseName, string status, string errorMessage, TimeSpan durationTime)
        {
            string sqlUpdateLog = @"
            UPDATE dbo.RestoreLogs
            SET 
                RestoreDate = @RestoreDate,
                RestoreDate_Shamsi = @RestoreDate_Shamsi,
                BackupFilePath = @BackupFilePath,
                Status = @Status,
                ResponseMessage = @ResponseMessage,
                DatabaseName = @DatabaseName,
                Duration = @Duration
            WHERE 
            LogId = @LogId";

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(sqlUpdateLog, connection))
                    {
                        command.Parameters.AddWithValue("@RestoreDate", DateTime.Now);
                        command.Parameters.AddWithValue("@RestoreDate_Shamsi", GregorianDateTimeToPersianDate(DateTime.Now));
                        command.Parameters.AddWithValue("@BackupFilePath", backupFilePath);
                        command.Parameters.AddWithValue("@Status", status);
                        command.Parameters.AddWithValue("@ResponseMessage", (object)errorMessage ?? DBNull.Value);
                        command.Parameters.AddWithValue("@DatabaseName", databaseName);
                        command.Parameters.AddWithValue("@Duration", TimeSpan.FromSeconds(Math.Round(durationTime.TotalSeconds)));
                        command.Parameters.AddWithValue("@LogId", restoreId);
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to update the restore attempt: " + ex.Message);
            }
        }
        static void SendSmsToPersonnels(string connectionString, string databaseName)
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
                    SmsLogInsert(connectionString, databaseName, false, "Zero Active Mobile Number ", null);
                    return;
                }
                string[] message = new string[mobileList.Count];
                for (int i = 0; i < message.Length; i++)
                {
                    message[i] += " فایل بک آپ ";
                    message[i] += databaseName;
                    message[i] += " در مورخ ";
                    message[i] += GregorianDateToPersianDate(DateTime.Now);
                    message[i] += " هنوز کپی نشده است !!! ";
                }
                RestClient restClient = new RestClient(Username, Password);
                restClient.SendMultipleSmartSMS(mobileList.ToArray(), message, "2184090", "10008409084090", "3000700290");
                SmsLogInsert(connectionString, databaseName, true, null, JsonConvert.SerializeObject(mobileList));
            }
            catch (Exception ex)
            {
                SmsLogInsert(connectionString, databaseName, false, ex.Message, JsonConvert.SerializeObject(mobileList));
                Console.WriteLine("Failed to send the sms: " + ex.Message);
            }
        }
        static void SmsLogInsert(string connectionString, string databaseName, bool isSuccessful, string? errorMessage, string? mobiles)
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
        static async Task<bool> IsHoliday(DateTime date)
        {
            string persianDate = GregorianDateToPersianDate(date);
            string url = $"https://holidayapi.ir/jalali/{persianDate.Replace("/", "/")}";
            using (HttpClient client = new HttpClient())
            {
                try
                {
                    HttpResponseMessage response = await client.GetAsync(url);
                    response.EnsureSuccessStatusCode();
                    string responseBody = await response.Content.ReadAsStringAsync();
                    JObject jsonResponse = JObject.Parse(responseBody);
                    bool isHoliday = jsonResponse["is_holiday"].Value<bool>();
                    return isHoliday;
                }
                catch (HttpRequestException e)
                {
                    Console.WriteLine($"Request error: {e.Message}");
                    return false;
                }
            }
        }
    }
}
