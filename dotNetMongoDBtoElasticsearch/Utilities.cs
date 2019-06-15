using System;
using System.Configuration;
using System.IO;
using System.Net.Mail;
using System.Text;

namespace dotNetMongoDBtoElasticsearch
{
    /// <summary>
    /// Utility methods for this program to use.
    /// </summary>
    public class Utilities
    {
        static string notifyTo = ConfigurationManager.AppSettings["EmailNotificTo"].ToString();
        static string notifySubject = ConfigurationManager.AppSettings["EmailNotificSubject"].ToString();
        public Utilities() { }

        /// <summary>
        /// Convert Unix Timestamp to DateTime
        /// </summary>
        /// <param name="timeStamp"></param>
        public string ConvertEpoch(Int64 timeStamp)
        {
            DateTime epochBegin = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            DateTime converted = epochBegin.AddMilliseconds(timeStamp).ToLocalTime();

            return converted.ToString("yyyy-MM-dd HH:mm:ss");
        }

        /// <summary>
        /// Convert Unix timestamp to DateTime with the option to specify DateTime to string format.
        /// </summary>
        /// <param name="timeStamp"></param>
        /// <param name="dateFormat"></param>
        /// <returns></returns>
        public string ConvertEpoch(Int64 timeStamp, string dateFormat)
        {
            DateTime epochBegin = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            DateTime converted = epochBegin.AddMilliseconds(timeStamp).ToLocalTime();

            return converted.ToString(dateFormat ?? "yyyy-MM-dd HH:mm:ss");
        }

        /// <summary>
        /// Save data to the file system.
        /// </summary>
        /// <param name="filePath">string</param>
        /// <param name="line">string</param>
        public void SaveToFile(string filePath, string line)
        {
            try
            {
                if (!File.Exists(filePath)) { File.Create(filePath).Dispose(); }

                using (StreamWriter sw = File.AppendText(filePath))
                {
                    sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "\t" + line);
                }
            }
            catch (Exception e)
            {
                Console.Write("\n\n+++ FILE/IO EXCEPTION +++\n\n" + e.Message.ToString() + "\n\n");
            }
        }

        /// <summary>
        /// Sends an email to a user or group (set in program configs) for notification purposes.
        /// </summary>
        /// <param name="emailMessage">string</param>
        public void SendEMail(string emailMessage)
        {
            ProcessEMail(notifyTo, notifySubject, emailMessage);
        }

        /// <summary>
        /// Sends an email that is customizable to a user or group for notification purposes.
        /// </summary>
        /// <param name="emailTo">string</param>
        /// <param name="emailSubject">string</param>
        /// <param name="emailMessage">string</param>
        public void SendEMail(string emailTo, string emailSubject, string emailMessage)
        {
            ProcessEMail(emailTo, emailSubject, emailMessage);
        }

        private void ProcessEMail(string emailTo, string emailSubject, string emailMessage)
        {
            SmtpClient client = new SmtpClient();
            client.Host = ConfigurationManager.AppSettings["SMTPServer"].ToString();
            client.Timeout = 10000;
            client.DeliveryMethod = SmtpDeliveryMethod.Network;

            string emailBody = string.Empty;
            emailBody += "\n";
            emailBody += "<b>" + emailSubject + "</b>\n\n";
            emailBody += "<b>" + emailMessage + "</b>\n";
            emailBody += "\n";

            MailMessage mm = new MailMessage("do-not-reply@mail.house.gov", emailTo, emailSubject, emailBody);
            mm.BodyEncoding = UTF8Encoding.UTF8;
            mm.DeliveryNotificationOptions = DeliveryNotificationOptions.OnFailure;

            client.Send(mm);
        }
    }
}
