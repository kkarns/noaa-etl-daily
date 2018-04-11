
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using System.Data.SqlClient;
using System.Threading;             // added for remote debugging  

// set to dotnet framework 4.6.1 or greater in the properties of the namespace

namespace NoaaEtlDaily
{
    class Program
    {
        /// <summary>
        /// 
        /// NoaaEtlDaily
        /// 
        /// An incremental daily load ETL program for daily NOAA radiation data 
        /// Note: Full loads are handled by another program in bulk.
        ///  
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            try
            {
                if (args.Length == 0)
                {
                    System.Console.WriteLine("Missing argument.  Please enter a date argument representing the age of the data.  This will be called NOAA_INCREMENTAL_DATE.");
                    System.Console.WriteLine("Usage: NoaaEtlDaily \"2017-08-02 00:00:00.000\"");
                    return;
                }

                // need to change this since NOAA uses DayOfYear instead of a date/time.

                DateTime argNoaaIncrementalDate;
                bool test = DateTime.TryParse(args[0], out argNoaaIncrementalDate);
                if (test == false)
                {
                    System.Console.WriteLine("Can't understand the date.  Please enter a date argument representing the age of the data.  This will be called NOAA_INCREMENTAL_DATE.");
                    System.Console.WriteLine("Usage: NoaaEtlDaily \"2017-08-02 00:00:00.000\"");
                    return;
                }


#if DEBUG       // combination of this source, and another https://stackoverflow.com/questions/361077/how-to-wait-until-remote-net-debugger-attached and https://stackoverflow.com/questions/5620603/non-blocking-read-from-standard-i-o-in-c-sharp
                Console.WriteLine("Waiting for debugger to attach, or press [F1] to bypass waiting for debugger.");
                Boolean isF1keydown = false;
                while (!(System.Diagnostics.Debugger.IsAttached | isF1keydown))
                {
                    if (Console.KeyAvailable)
                    {
                        ConsoleKeyInfo key = Console.ReadKey(true);
                        switch (key.Key)
                        {
                            case ConsoleKey.F1:
                                isF1keydown = true;
                                break;
                            default:
                                break;
                        }
                    }
                    Thread.Sleep(100);
                }
                if (isF1keydown) { Console.WriteLine("You pressed F1!, bypassing the debugger."); }
                else { Console.WriteLine("Debugger attached."); }
#endif

                Console.WriteLine("ETL code for NOAA datafile...");

                // call raw loader
                //if (!Loader())
                //{
                //    throw new System.Exception("error encountered during Loader");
                //}

                // call table prep code
                if (!Prepare())
                {
                    throw new System.Exception("error encountered during Prepare");
                }

                // call cleaning code
                if (!Cleaner(argNoaaIncrementalDate))
                {
                    throw new System.Exception("error encountered during Cleaner");
                }

                // call unduplicating code
                if (!Unduplicator(argNoaaIncrementalDate))
                {
                    throw new System.Exception("error encountered during Unduplicator");
                }

                // call referential integrity code
                //
                //     SurfRad is a master table ... no RI code for this table 
                //

                // call final load
                if (!FinalLoader(argNoaaIncrementalDate))
                {
                    throw new System.Exception("error encountered during Final Loader");
                }


#if DEBUG
                Console.WriteLine("\nHit Enter key to continue...");
                Console.ReadLine();
#endif

            }
            catch (System.Exception e)
            {
                Console.WriteLine("    Caught an exception: " + e.Source + " " + e.Message + " " + e.StackTrace);
            }

        }


        /// <summary>
        ///  Loader is currently in SSIS and involves some simple, but manual effort.
        ///  The loader should always be "safe" and do whatever it takes to load the data "as-is" from the source 
        ///  even if it means oversizing fields or leaving as nvarchar
        /// </summary>
        /// <returns></returns>
        static bool Loader()
        {
            try
            {

            }
            catch (System.Exception e)
            {
                Console.WriteLine("    Caught an exception: " + e.Source + " " + e.Message + " " + e.StackTrace);
                return false;
            }
            return true;
        }

        /// <summary>
        ///  Counter just counts the number of rows in the table
        /// </summary>
        /// <returns></returns>
        static int Counter(string tableName)
        {

            int nRows = 0;

            try
            {
                SqlConnectionStringBuilder strBuilderRead = new SqlConnectionStringBuilder();
                strBuilderRead.DataSource = "127.0.0.1,31433";
                strBuilderRead.InitialCatalog = "noaaC";
                //strBuilderRead.IntegratedSecurity = true;
                strBuilderRead.UserID = "sa";
                strBuilderRead.Password = "omg.passwords!";
                strBuilderRead.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;

                using (SqlConnection sqlCountConnection = new SqlConnection(strBuilderRead.ConnectionString))
                {
                    sqlCountConnection.Open();
                    using (SqlCommand cmd = sqlCountConnection.CreateCommand())
                    {
                        cmd.CommandText = @"SELECT count(*) FROM " + tableName;
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    nRows = (int)reader[0];
                                    Console.WriteLine(@"{0} rows in the " + tableName + " table", reader[0]);
                                }
                            }
                        }
                    }

                    sqlCountConnection.Close();
                    sqlCountConnection.Dispose();
                }

            }
            catch (System.Exception e)
            {
                Console.WriteLine("    Caught an exception: " + e.Source + " " + e.Message + " " + e.StackTrace);
                return -1;
            }
            return nRows;
        }


        /// <summary>
        /// The Prepare() method gets temporary tables ready for ETL processing 
        /// </summary>
        /// <returns></returns>
        static bool Prepare()
        {
            try
            {
                SqlConnectionStringBuilder strBuilderRead = new SqlConnectionStringBuilder();
                strBuilderRead.DataSource = "127.0.0.1,31433";
                strBuilderRead.InitialCatalog = "noaaC";
                //strBuilderRead.IntegratedSecurity = true;
                strBuilderRead.UserID = "sa";
                strBuilderRead.Password = "omg.passwords!";
                strBuilderRead.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;

                using (SqlConnection sqlPrepConnection = new SqlConnection(strBuilderRead.ConnectionString))
                {
                    sqlPrepConnection.Open();
                    using (SqlCommand cmd = sqlPrepConnection.CreateCommand())
                    {
                        cmd.CommandText = @"TRUNCATE TABLE _in_clean_SurfRad";
                        cmd.ExecuteNonQuery();
                        Console.WriteLine(@"truncated table _in_clean_SurfRad");
                    }

                    using (SqlCommand cmd = sqlPrepConnection.CreateCommand())
                    {
                        cmd.CommandText = @"TRUNCATE TABLE _in_dups_SurfRad";
                        cmd.ExecuteNonQuery();
                        Console.WriteLine(@"truncated table _in_dups_SurfRad");
                    }

                    using (SqlCommand cmd = sqlPrepConnection.CreateCommand())
                    {
                        cmd.CommandText = @"TRUNCATE TABLE _in_group_SurfRad";
                        cmd.ExecuteNonQuery();
                        Console.WriteLine(@"truncated table _in_group_SurfRad");
                    }

                    using (SqlCommand cmd = sqlPrepConnection.CreateCommand())
                    {
                        cmd.CommandText = @"TRUNCATE TABLE _in_SurfRad";
                        cmd.ExecuteNonQuery();
                        Console.WriteLine(@"truncated table _in_SurfRad");
                    }

                    sqlPrepConnection.Close();
                    sqlPrepConnection.Dispose();
                }

            }
            catch (System.Exception e)
            {
                Console.WriteLine("    Caught an exception: " + e.Source + " " + e.Message + " " + e.StackTrace);
                return false;
            }
            return true;
        }


        /// <summary>
        ///  Cleaner utilizes the SQL Server Always Encrypted libraries in .Net 4.6.1 to clean data.
        ///  This includes handling nulls, and converting to proper types. 
        ///  Or in the case of encryption: calculating a wildcard field in place of PII or protected data 
        ///  like "last 4 digits" of ssn or credit card, or age.
        ///  
        ///  This cleaner is adding the NOAA_INCREMENTAL_DATE to the table
        /// </summary>
        /// <returns></returns>
        static bool Cleaner(DateTime argNoaaIncrementalDate)
        {
            try
            {
                // values needed for aggregator before work begins                   
                DateTime etlStartTime = DateTime.Now;
                int nRecords = 0;
                String tableName = "_in_clean_SurfRad";

                // connection strings 
                SqlConnectionStringBuilder strBuilderRead = new SqlConnectionStringBuilder();
                strBuilderRead.DataSource = "127.0.0.1,31433";
                strBuilderRead.InitialCatalog = "noaaC";
                //strBuilderRead.IntegratedSecurity = true;
                strBuilderRead.UserID = "sa";
                strBuilderRead.Password = "omg.passwords!";
                strBuilderRead.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;

                SqlConnectionStringBuilder strBuilderWrite = new SqlConnectionStringBuilder();
                strBuilderWrite.DataSource = "127.0.0.1,31433";
                strBuilderRead.InitialCatalog = "noaaC";
                //strBuilderRead.IntegratedSecurity = true;
                strBuilderWrite.UserID = "sa";
                strBuilderWrite.Password = "omg.passwords!";
                strBuilderWrite.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;

                // next .... select from _raw ... insert to _clean

                using (SqlConnection sqlReadConnection = new SqlConnection(strBuilderRead.ConnectionString))
                {
                    sqlReadConnection.Open();
                    using (SqlCommand sqlReadCmd = sqlReadConnection.CreateCommand())
                    {
                        sqlReadCmd.CommandText = @"SELECT 
                              noaa_year      , --  int       NULL,      -- year, i.e., 1995                                         Column0     varchar(5)      2018    
                              noaa_jday      , --  int       NULL,      -- Julian day (1 through 365 [or 366])                      Column1     varchar(4)      1  
                              noaa_month     , --  int       NULL,      -- number of the month (1-12)                               Column2     varchar(3)      1  
                              noaa_day       , --  int       NULL,      -- day of the month(1-31)                                   Column3     varchar(3)      1  
                              noaa_hour      , --  int       NULL,      -- hour of the day (0-23)                                   Column4     varchar(3)      0  
                              noaa_min       , --  int       NULL,      -- minute of the hour (0-59)                                Column5     varchar(3)      0  
                              dt             , --  real      NULL,      -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7)      0.000  
                              zen            , --  real      NULL,      -- solar zenith angle (degrees)                             Column7     varchar(7)      93.20     
                              dw_solar       , --  real      NULL,      -- downwelling global solar (Watts m^-2)                    Column8     varchar(8)      0.0 
                              dw_solar_qc    , --  int       NULL,      --                                                          Column9     varchar(2)      0     
                              uw_solar       , --  real      NULL,      -- upwelling global solar (Watts m^-2)                      Column10    varchar(8)      0.4 
                              uw_solar_qc    , --  int       NULL,      --                                                          Column11    varchar(2)      0     
                              direct_n       , --  real      NULL,      -- direct-normal solar (Watts m^-2)                         Column12    varchar(8)      0.8 
                              direct_n_qc    , --  int       NULL,      --                                                          Column13    varchar(2)      0     
                              diffuse        , --  real      NULL,      -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8)      0.7 
                              diffuse_qc     , --  int       NULL,      --                                                          Column15    varchar(2)      0   
                              dw_ir          , --  real      NULL,      -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8)      239.3 
                              dw_ir_qc       , --  int       NULL,      --                                                          Column17    varchar(2)      0   
                              dw_casetemp    , --  real      NULL,      -- downwelling IR case temp. (K)                            Column18    varchar(9)      262.88 
                              dw_casetemp_qc , --  int       NULL,      --                                                          Column19    varchar(2)      0   
                              dw_dometemp    , --  real      NULL,      -- downwelling IR dome temp. (K)                            Column20    varchar(9)      262.79 
                              dw_dometemp_qc , --  int       NULL,      --                                                          Column21    varchar(2)      0   
                              uw_ir          , --  real      NULL,      -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8)      272.9 
                              uw_ir_qc       , --  int       NULL,      --                                                          Column23    varchar(2)      0   
                              uw_casetemp    , --  real      NULL,      -- upwelling IR case temp. (K)                              Column24    varchar(9)      262.34 
                              uw_casetemp_qc , --  int       NULL,      --                                                          Column25    varchar(2)      0   
                              uw_dometemp    , --  real      NULL,      -- upwelling IR dome temp. (K)                              Column26    varchar(9)      262.23 
                              uw_dometemp_qc , --  int       NULL,      --                                                          Column27    varchar(2)      0     
                              uvb            , --  real      NULL,      -- global UVB (milliWatts m^-2)                             Column28    varchar(8)      0.0 
                              uvb_qc         , --  int       NULL,      --                                                          Column29    varchar(2)      0     
                              par            , --  real      NULL,      -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8)      0.4 
                              par_qc         , --  int       NULL,      --                                                          Column31    varchar(2)      0     
                              netsolar       , --  real      NULL,      -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8)      0.4 
                              netsolar_qc    , --  int       NULL,      --                                                          Column33    varchar(2)      0   
                              netir          , --  real      NULL,      -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8)      -33.6 
                              netir_qc       , --  int       NULL,      --                                                          Column35    varchar(2)      0   
                              totalnet       , --  real      NULL,      -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8)      -33.2 
                              totalnet_qc    , --  int       NULL,      --                                                          Column37    varchar(2)      0   
                              temp           , --  real      NULL,      -- 10-meter air temperature (?C)                            Column38    varchar(8)      -10.4 
                              temp_qc        , --  int       NULL,      --                                                          Column39    varchar(2)      0    
                              rh             , --  real      NULL,      -- relative humidity (%)                                    Column40    varchar(8)      92.1 
                              rh_qc          , --  int       NULL,      --                                                          Column41    varchar(2)      0     
                              windspd        , --  real      NULL,      -- wind speed (ms^-1)                                       Column42    varchar(8)      0.8 
                              windspd_qc     , --  int       NULL,      --                                                          Column43    varchar(2)      0   
                              winddir        , --  real      NULL,      -- wind direction (degrees, clockwise from north)           Column44    varchar(8)      110.1 
                              winddir_qc     , --  int       NULL,      --                                                          Column45    varchar(2)      0   
                              pressure       , --  real      NULL,      -- station pressure (mb)                                    Column46    varchar(8)      837.1 
                              pressure_qc      --  int       NULL       --                                                          Column47    varchar(2)      0
                        FROM _in_raw_SurfRad;";

                        using (SqlDataReader reader = sqlReadCmd.ExecuteReader(CommandBehavior.SequentialAccess))
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    //Console.WriteLine(@"{0} {1}", reader[0], reader[13]);

                                    var noaa_year = (reader[0] as Int32?) ?? 0;                   // int       NULL,     -- year, i.e., 1995                                         Column0     varchar(5) 
                                    var noaa_jday = (reader[1] as Int32?) ?? 0;                   // int       NULL,     -- Julian day (1 through 365 [or 366])                      Column1     varchar(4) 
                                    var noaa_month = (reader[2] as Int32?) ?? 0;                   // int       NULL,     -- number of the month (1-12)                               Column2     varchar(3) 
                                    var noaa_day = (reader[3] as Int32?) ?? 0;                   // int       NULL,     -- day of the month(1-31)                                   Column3     varchar(3) 
                                    var noaa_hour = (reader[4] as Int32?) ?? 0;                   // int       NULL,     -- hour of the day (0-23)                                   Column4     varchar(3) 
                                    var noaa_min = (reader[5] as Int32?) ?? 0;                   // int       NULL,     -- minute of the hour (0-59)                                Column5     varchar(3) 
                                    var dt = (reader[6] as Decimal?) ?? 0;                 // real      NULL,     -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7) 
                                    var zen = (reader[7] as Decimal?) ?? 0;                 // real      NULL,     -- solar zenith angle (degrees)                             Column7     varchar(7) 
                                    var dw_solar = (reader[8] as Decimal?) ?? 0;                 // real      NULL,     -- downwelling global solar (Watts m^-2)                    Column8     varchar(8) 
                                    var dw_solar_qc = (reader[9] as Int32?) ?? 0;                   // int       NULL,     --                                                          Column9     varchar(2) 
                                    var uw_solar = (reader[10] as Decimal?) ?? 0;                // real      NULL,     -- upwelling global solar (Watts m^-2)                      Column10    varchar(8) 
                                    var uw_solar_qc = (reader[11] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column11    varchar(2) 
                                    var direct_n = (reader[12] as Decimal?) ?? 0;                // real      NULL,     -- direct-normal solar (Watts m^-2)                         Column12    varchar(8) 
                                    var direct_n_qc = (reader[13] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column13    varchar(2) 
                                    var diffuse = (reader[14] as Decimal?) ?? 0;                // real      NULL,     -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8) 
                                    var diffuse_qc = (reader[15] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column15    varchar(2) 
                                    var dw_ir = (reader[16] as Decimal?) ?? 0;                // real      NULL,     -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8) 
                                    var dw_ir_qc = (reader[17] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column17    varchar(2) 
                                    var dw_casetemp = (reader[18] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR case temp. (K)                            Column18    varchar(9) 
                                    var dw_casetemp_qc = (reader[19] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column19    varchar(2) 
                                    var dw_dometemp = (reader[20] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR dome temp. (K)                            Column20    varchar(9) 
                                    var dw_dometemp_qc = (reader[21] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column21    varchar(2) 
                                    var uw_ir = (reader[22] as Decimal?) ?? 0;                // real      NULL,     -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8) 
                                    var uw_ir_qc = (reader[23] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column23    varchar(2) 
                                    var uw_casetemp = (reader[24] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR case temp. (K)                              Column24    varchar(9) 
                                    var uw_casetemp_qc = (reader[25] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column25    varchar(2) 
                                    var uw_dometemp = (reader[26] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR dome temp. (K)                              Column26    varchar(9) 
                                    var uw_dometemp_qc = (reader[27] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column27    varchar(2) 
                                    var uvb = (reader[28] as Decimal?) ?? 0;                // real      NULL,     -- global UVB (milliWatts m^-2)                             Column28    varchar(8) 
                                    var uvb_qc = (reader[29] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column29    varchar(2) 
                                    var par = (reader[30] as Decimal?) ?? 0;                // real      NULL,     -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8) 
                                    var par_qc = (reader[31] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column31    varchar(2) 
                                    var netsolar = (reader[32] as Decimal?) ?? 0;                // real      NULL,     -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8) 
                                    var netsolar_qc = (reader[33] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column33    varchar(2) 
                                    var netir = (reader[34] as Decimal?) ?? 0;                // real      NULL,     -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8) 
                                    var netir_qc = (reader[35] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column35    varchar(2) 
                                    var totalnet = (reader[36] as Decimal?) ?? 0;                // real      NULL,     -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8) 
                                    var totalnet_qc = (reader[37] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column37    varchar(2) 
                                    var temp = (reader[38] as Decimal?) ?? 0;                // real      NULL,     -- 10-meter air temperature (?C)                            Column38    varchar(8) 
                                    var temp_qc = (reader[39] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column39    varchar(2) 
                                    var rh = (reader[40] as Decimal?) ?? 0;                // real      NULL,     -- relative humidity (%)                                    Column40    varchar(8) 
                                    var rh_qc = (reader[41] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column41    varchar(2) 
                                    var windspd = (reader[42] as Decimal?) ?? 0;                // real      NULL,     -- wind speed (ms^-1)                                       Column42    varchar(8) 
                                    var windspd_qc = (reader[43] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column43    varchar(2) 
                                    var winddir = (reader[44] as Decimal?) ?? 0;                // real      NULL,     -- wind direction (degrees, clockwise from north)           Column44    varchar(8) 
                                    var winddir_qc = (reader[45] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column45    varchar(2) 
                                    var pressure = (reader[46] as Decimal?) ?? 0;                // real      NULL,     -- station pressure (mb)                                    Column46    varchar(8) 
                                    var pressure_qc = (reader[47] as Int32?) ?? 0;                  // int       NULL      --                                                          Column47    varchar(2) 


                                    // handle extra fields for wildcard or PII support
                                    // including wildcard search and query fields that mitigate PII constraints  (in this example age is less sensitive than dob)   
                                    // var ageAsOfDay = DateTime.Today;          // or better yet, .Now            // todo: will need to get the age of the data rather than .Today
                                    // var ageAsOfDay = DateTime.ParseExact("2017-05-31", "yyyy-MM-dd", null);     // todo: will need to get the age of the data rather than hardcoding

                                    // handle extra fields related to processing ... introduced here at _in_clean because initial load might be a simple SSIS import.
                                    var fileDate = DateTime.Now;                // file_date       varchar(20) NULL,    -- noaa filedate (encoded from DayOfYear)                
                                    var addedDate = argNoaaIncrementalDate;     // added_date      datetime  NULL       -- date data added to database, often delayed 

                                    using (SqlConnection sqlWriteConnection = new SqlConnection(strBuilderWrite.ConnectionString))
                                    {

                                        sqlWriteConnection.Open();
                                        using (SqlCommand sqlWriteCmd = sqlWriteConnection.CreateCommand())
                                        {
                                            sqlWriteCmd.CommandText = @"INSERT INTO _in_clean_SurfRad (
                                                noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min, dt, zen, dw_solar, dw_solar_qc, uw_solar, uw_solar_qc, direct_n, direct_n_qc, diffuse, diffuse_qc, dw_ir, dw_ir_qc, dw_casetemp, dw_casetemp_qc, dw_dometemp, dw_dometemp_qc, uw_ir, uw_ir_qc, uw_casetemp, uw_casetemp_qc, uw_dometemp, uw_dometemp_qc, uvb, uvb_qc, par, par_qc, netsolar, netsolar_qc, netir, netir_qc, totalnet, totalnet_qc, temp, temp_qc, rh, rh_qc, windspd, windspd_qc, winddir, winddir_qc, pressure, pressure_qc, fileDate, addedDate
                                                ) 
                                                VALUES 
                                                (
                                                @noaa_year, @noaa_jday, @noaa_month, @noaa_day, @noaa_hour, @noaa_min, @dt, @zen, @dw_solar, @dw_solar_qc, @uw_solar, @uw_solar_qc, @direct_n, @direct_n_qc, @diffuse, @diffuse_qc, @dw_ir, @dw_ir_qc, @dw_casetemp, @dw_casetemp_qc, @dw_dometemp, @dw_dometemp_qc, @uw_ir, @uw_ir_qc, @uw_casetemp, @uw_casetemp_qc, @uw_dometemp, @uw_dometemp_qc, @uvb, @uvb_qc, @par, @par_qc, @netsolar, @netsolar_qc, @netir, @netir_qc, @totalnet, @totalnet_qc, @temp, @temp_qc, @rh, @rh_qc, @windspd, @windspd_qc, @winddir, @winddir_qc, @pressure, @pressure_qc, @fileDate, @addedDate
                                                )";

                                            sqlWriteCmd.Parameters.Add("@noaa_year      ", SqlDbType.Int, 12).Value = noaa_year;
                                            sqlWriteCmd.Parameters.Add("@noaa_jday      ", SqlDbType.Int, 12).Value = noaa_jday;
                                            sqlWriteCmd.Parameters.Add("@noaa_month     ", SqlDbType.Int, 12).Value = noaa_month;
                                            sqlWriteCmd.Parameters.Add("@noaa_day       ", SqlDbType.Int, 12).Value = noaa_day;
                                            sqlWriteCmd.Parameters.Add("@noaa_hour      ", SqlDbType.Int, 12).Value = noaa_hour;
                                            sqlWriteCmd.Parameters.Add("@noaa_min       ", SqlDbType.Int, 12).Value = noaa_min;
                                            sqlWriteCmd.Parameters.Add("@dt", SqlDbType.Decimal, 16).Value = dt;
                                            sqlWriteCmd.Parameters["@dt"].Precision = 7;
                                            sqlWriteCmd.Parameters["@dt"].Scale = 3;
                                            sqlWriteCmd.Parameters.Add("@zen", SqlDbType.Decimal, 16).Value = zen;
                                            sqlWriteCmd.Parameters["@zen"].Precision = 7;
                                            sqlWriteCmd.Parameters["@zen"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_solar", SqlDbType.Decimal, 16).Value = dw_solar;
                                            sqlWriteCmd.Parameters["@dw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_solar_qc    ", SqlDbType.Int, 12).Value = dw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_solar", SqlDbType.Decimal, 16).Value = uw_solar;
                                            sqlWriteCmd.Parameters["@uw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_solar_qc", SqlDbType.Int, 12).Value = uw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@direct_n", SqlDbType.Decimal, 16).Value = direct_n;
                                            sqlWriteCmd.Parameters["@direct_n"].Precision = 8;
                                            sqlWriteCmd.Parameters["@direct_n"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@direct_n_qc", SqlDbType.Int, 12).Value = direct_n_qc;
                                            sqlWriteCmd.Parameters.Add("@diffuse", SqlDbType.Decimal, 16).Value = diffuse;
                                            sqlWriteCmd.Parameters["@diffuse"].Precision = 8;
                                            sqlWriteCmd.Parameters["@diffuse"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@diffuse_qc", SqlDbType.Int, 12).Value = diffuse_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_ir", SqlDbType.Decimal, 16).Value = dw_ir;
                                            sqlWriteCmd.Parameters["@dw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_ir_qc", SqlDbType.Int, 12).Value = dw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp", SqlDbType.Decimal, 16).Value = dw_casetemp;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp_qc", SqlDbType.Int, 12).Value = dw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp", SqlDbType.Decimal, 16).Value = dw_dometemp;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp_qc", SqlDbType.Int, 12).Value = dw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_ir", SqlDbType.Decimal, 16).Value = uw_ir;
                                            sqlWriteCmd.Parameters["@uw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_ir_qc", SqlDbType.Int, 12).Value = uw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp", SqlDbType.Decimal, 16).Value = uw_casetemp;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp_qc", SqlDbType.Int, 12).Value = uw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp", SqlDbType.Decimal, 16).Value = uw_dometemp;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp_qc", SqlDbType.Int, 12).Value = uw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uvb", SqlDbType.Decimal, 16).Value = uvb;
                                            sqlWriteCmd.Parameters["@uvb"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uvb"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uvb_qc", SqlDbType.Int, 12).Value = uvb_qc;
                                            sqlWriteCmd.Parameters.Add("@par", SqlDbType.Decimal, 16).Value = par;
                                            sqlWriteCmd.Parameters["@par"].Precision = 8;
                                            sqlWriteCmd.Parameters["@par"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@par_qc", SqlDbType.Int, 12).Value = par_qc;
                                            sqlWriteCmd.Parameters.Add("@netsolar", SqlDbType.Decimal, 16).Value = netsolar;
                                            sqlWriteCmd.Parameters["@netsolar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netsolar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netsolar_qc", SqlDbType.Int, 12).Value = netsolar_qc;
                                            sqlWriteCmd.Parameters.Add("@netir", SqlDbType.Decimal, 16).Value = netir;
                                            sqlWriteCmd.Parameters["@netir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netir_qc", SqlDbType.Int, 12).Value = netir_qc;
                                            sqlWriteCmd.Parameters.Add("@totalnet", SqlDbType.Decimal, 16).Value = totalnet;
                                            sqlWriteCmd.Parameters["@totalnet"].Precision = 8;
                                            sqlWriteCmd.Parameters["@totalnet"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@totalnet_qc", SqlDbType.Int, 12).Value = totalnet_qc;
                                            sqlWriteCmd.Parameters.Add("@temp", SqlDbType.Decimal, 16).Value = temp;
                                            sqlWriteCmd.Parameters["@temp"].Precision = 8;
                                            sqlWriteCmd.Parameters["@temp"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@temp_qc", SqlDbType.Int, 12).Value = temp_qc;
                                            sqlWriteCmd.Parameters.Add("@rh", SqlDbType.Decimal, 16).Value = rh;
                                            sqlWriteCmd.Parameters["@rh"].Precision = 8;
                                            sqlWriteCmd.Parameters["@rh"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@rh_qc", SqlDbType.Int, 12).Value = rh_qc;
                                            sqlWriteCmd.Parameters.Add("@windspd", SqlDbType.Decimal, 16).Value = windspd;
                                            sqlWriteCmd.Parameters["@windspd"].Precision = 8;
                                            sqlWriteCmd.Parameters["@windspd"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@windspd_qc", SqlDbType.Int, 12).Value = windspd_qc;
                                            sqlWriteCmd.Parameters.Add("@winddir", SqlDbType.Decimal, 16).Value = winddir;
                                            sqlWriteCmd.Parameters["@winddir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@winddir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@winddir_qc", SqlDbType.Int, 12).Value = winddir_qc;
                                            sqlWriteCmd.Parameters.Add("@pressure", SqlDbType.Decimal, 16).Value = pressure;
                                            sqlWriteCmd.Parameters["@pressure"].Precision = 8;
                                            sqlWriteCmd.Parameters["@pressure"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@pressure_qc", SqlDbType.Int, 12).Value = pressure_qc;


                                            //Console.WriteLine(@"ready to write to _clean {0} {1} {2} {3}", accountKey, DOB, age, ageAsOfDay);
                                            //sqlWriteCmd.CommandTimeout = 0; // for debugging only ... turns out the problem lied with nulls and nvarchar vs varchar
                                            sqlWriteCmd.ExecuteNonQuery();
                                            //Console.WriteLine(@"wrote to _clean {0} {1} {2} {3}", accountKey, DOB, age, ageAsOfDay);


                                        }
                                        sqlWriteConnection.Close();
                                    }
                                }
                            }
                        }
                    }
                    sqlReadConnection.Close();
                }

                // values needed for aggregator after work complete                   
                DateTime etlEndTime = DateTime.Now;
                nRecords = Counter(tableName);
                if (!Aggregator(etlStartTime, etlEndTime, tableName, nRecords, argNoaaIncrementalDate))
                {
                    throw new System.Exception("error encountered during Aggregator");
                }


            }
            catch (System.Exception e)
            {
                Console.WriteLine("    Caught an exception: " + e.Source + " " + e.Message + " " + e.StackTrace);
                return false;
            }
            return true;
        }


        /// <summary>
        ///  Unduplicator utilizes the SQL Server Always Encrypted libraries in .Net 4.6.1 to remove duplicate data.
        ///  It tries to model the unduplicator pattern modeled in the paper by Veronica Peralta
        ///  "Extraction and Integration of MovieLens and IMDb Data".  
        ///  However, SQL Server Always Encrypted prevents us from using server side insert and update queries
        ///  and aggregations, so we model the behavior using loops and dictionary objects in c#.
        ///  Another change to her pattern is that this code leaves the _clean tables intact with the original data
        ///  if needed for qa/qc, and instead ... inserts into the final-grouped table in two steps 
        ///  (first from _dups with one each of the dups, then non-dup data from from _clean) 
        ///  instead of one insert from _clean.
        /// </summary>
        /// <returns></returns>
        static bool Unduplicator(DateTime argNoaaIncrementalDate)
        {
            try
            {
                // values needed for aggregator before work begins                   
                DateTime etlStartTime = DateTime.Now;
                int nRecords = 0;
                String tableName = "_in_dups_SurfRad";

                // connection strings 
                SqlConnectionStringBuilder strBuilderRead = new SqlConnectionStringBuilder();
                strBuilderRead.DataSource = "127.0.0.1,31433";
                strBuilderRead.InitialCatalog = "noaaC";
                //strBuilderRead.IntegratedSecurity = true;
                strBuilderRead.UserID = "sa";
                strBuilderRead.Password = "omg.passwords!";
                strBuilderRead.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;

                SqlConnectionStringBuilder strBuilderWrite = new SqlConnectionStringBuilder();
                strBuilderWrite.DataSource = "127.0.0.1,31433";
                strBuilderRead.InitialCatalog = "noaaC";
                //strBuilderRead.IntegratedSecurity = true;
                strBuilderWrite.UserID = "sa";
                strBuilderWrite.Password = "omg.passwords!";
                strBuilderWrite.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;


                // next .... simulate a group by query to identify the duplicates ... but max and min won't work to identify diffs in the duplicates
                // however, we can sort by the key on the server side this time ... and that runs in 48 seconds and handles a lot of the effort
                // remaining effort is to track the adjacent duplicate rows, and track max and mins manually until we've left the set of adjacencies

                using (SqlConnection sqlReadConnection = new SqlConnection(strBuilderRead.ConnectionString))
                {
                    sqlReadConnection.Open();
                    using (SqlCommand sqlReadCmd = sqlReadConnection.CreateCommand())
                    {
                        sqlReadCmd.CommandText = @"SELECT 
                            noaa_year      , --  int       NULL,      -- year, i.e., 1995                                         Column0     varchar(5)      2018    
                            noaa_jday      , --  int       NULL,      -- Julian day (1 through 365 [or 366])                      Column1     varchar(4)      1  
                            noaa_month     , --  int       NULL,      -- number of the month (1-12)                               Column2     varchar(3)      1  
                            noaa_day       , --  int       NULL,      -- day of the month(1-31)                                   Column3     varchar(3)      1  
                            noaa_hour      , --  int       NULL,      -- hour of the day (0-23)                                   Column4     varchar(3)      0  
                            noaa_min       , --  int       NULL,      -- minute of the hour (0-59)                                Column5     varchar(3)      0  
                            dt             , --  real      NULL,      -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7)      0.000  
                            zen            , --  real      NULL,      -- solar zenith angle (degrees)                             Column7     varchar(7)      93.20     
                            dw_solar       , --  real      NULL,      -- downwelling global solar (Watts m^-2)                    Column8     varchar(8)      0.0 
                            dw_solar_qc    , --  int       NULL,      --                                                          Column9     varchar(2)      0     
                            uw_solar       , --  real      NULL,      -- upwelling global solar (Watts m^-2)                      Column10    varchar(8)      0.4 
                            uw_solar_qc    , --  int       NULL,      --                                                          Column11    varchar(2)      0     
                            direct_n       , --  real      NULL,      -- direct-normal solar (Watts m^-2)                         Column12    varchar(8)      0.8 
                            direct_n_qc    , --  int       NULL,      --                                                          Column13    varchar(2)      0     
                            diffuse        , --  real      NULL,      -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8)      0.7 
                            diffuse_qc     , --  int       NULL,      --                                                          Column15    varchar(2)      0   
                            dw_ir          , --  real      NULL,      -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8)      239.3 
                            dw_ir_qc       , --  int       NULL,      --                                                          Column17    varchar(2)      0   
                            dw_casetemp    , --  real      NULL,      -- downwelling IR case temp. (K)                            Column18    varchar(9)      262.88 
                            dw_casetemp_qc , --  int       NULL,      --                                                          Column19    varchar(2)      0   
                            dw_dometemp    , --  real      NULL,      -- downwelling IR dome temp. (K)                            Column20    varchar(9)      262.79 
                            dw_dometemp_qc , --  int       NULL,      --                                                          Column21    varchar(2)      0   
                            uw_ir          , --  real      NULL,      -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8)      272.9 
                            uw_ir_qc       , --  int       NULL,      --                                                          Column23    varchar(2)      0   
                            uw_casetemp    , --  real      NULL,      -- upwelling IR case temp. (K)                              Column24    varchar(9)      262.34 
                            uw_casetemp_qc , --  int       NULL,      --                                                          Column25    varchar(2)      0   
                            uw_dometemp    , --  real      NULL,      -- upwelling IR dome temp. (K)                              Column26    varchar(9)      262.23 
                            uw_dometemp_qc , --  int       NULL,      --                                                          Column27    varchar(2)      0     
                            uvb            , --  real      NULL,      -- global UVB (milliWatts m^-2)                             Column28    varchar(8)      0.0 
                            uvb_qc         , --  int       NULL,      --                                                          Column29    varchar(2)      0     
                            par            , --  real      NULL,      -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8)      0.4 
                            par_qc         , --  int       NULL,      --                                                          Column31    varchar(2)      0     
                            netsolar       , --  real      NULL,      -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8)      0.4 
                            netsolar_qc    , --  int       NULL,      --                                                          Column33    varchar(2)      0   
                            netir          , --  real      NULL,      -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8)      -33.6 
                            netir_qc       , --  int       NULL,      --                                                          Column35    varchar(2)      0   
                            totalnet       , --  real      NULL,      -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8)      -33.2 
                            totalnet_qc    , --  int       NULL,      --                                                          Column37    varchar(2)      0   
                            temp           , --  real      NULL,      -- 10-meter air temperature (?C)                            Column38    varchar(8)      -10.4 
                            temp_qc        , --  int       NULL,      --                                                          Column39    varchar(2)      0    
                            rh             , --  real      NULL,      -- relative humidity (%)                                    Column40    varchar(8)      92.1 
                            rh_qc          , --  int       NULL,      --                                                          Column41    varchar(2)      0     
                            windspd        , --  real      NULL,      -- wind speed (ms^-1)                                       Column42    varchar(8)      0.8 
                            windspd_qc     , --  int       NULL,      --                                                          Column43    varchar(2)      0   
                            winddir        , --  real      NULL,      -- wind direction (degrees, clockwise from north)           Column44    varchar(8)      110.1 
                            winddir_qc     , --  int       NULL,      --                                                          Column45    varchar(2)      0   
                            pressure       , --  real      NULL,      -- station pressure (mb)                                    Column46    varchar(8)      837.1 
                            pressure_qc    , --  int       NULL       --                                                          Column47    varchar(2)      0
                            file_date      , --  datetime  NULL,      -- noaa filedate (encoded from DayOfYear)    
                            added_date       --  datetime  NULL       -- date data added to database, often delayed
                        FROM _in_clean_SurfRad
                        ORDER BY 
                            noaa_year      ,
                            noaa_jday      ,
                            noaa_month     ,
                            noaa_day       ,
                            noaa_hour      ,
                            noaa_min;";
                        using (SqlDataReader reader = sqlReadCmd.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                // query ordered by (noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min) .. so we'll use the next three objects to build the dupSurfRadObject list 
                                String lastSurfRad = "-1";                                       // low mark value that doesn't exist in the dataset 
                                var lastSurfRadObject = new Dictionary<string, object>();           // hang on to the last SurfRad object until we know if it's the first record of dup surfrad
                                int currentDupCount = 0;                                            // increase this when we hit a dup SurfRad, keep increasing

                                var dupSurfRadObjectList = new List<Dictionary<string, object>>();  // a list of SurfRad objects that have duplicate uniquekeys

                                while (reader.Read())
                                {
                                    // source: https://stackoverflow.com/questions/912948/sqldatareader-how-to-convert-the-current-row-to-a-dictionary
                                    // Convert current row of the query into a dictionary
                                    var surfRadObject = new Dictionary<string, object>();
                                    for (int i = 0; i < reader.FieldCount; i++)
                                    {
                                        surfRadObject.Add(reader.GetName(i), reader.GetValue(i));
                                    }

                                    // our criteria for duplicates are based only on duplicate surfrad keysets
                                    // we could have used a different criteria to say that duplicates are only rows where ALL columns match, not just the duplicate keysets
                                    object surfRadKeyObject = "";
                                    Boolean hasSurfRadKey = surfRadObject.TryGetValue("noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min", out surfRadKeyObject);   // pull the surfRadKey value out of the surfRadObject dictionary
                                    string surfRadKey = surfRadKeyObject.ToString();

                                    if (surfRadKey == lastSurfRad)
                                    {
                                        // we found the nth record of a duplicate 
                                        currentDupCount++;
                                        if (currentDupCount == 1)
                                        {
                                            //go back and add the first SurfRad object to the dup list
                                            dupSurfRadObjectList.Add(lastSurfRadObject);

                                        }
                                        // add this duplicate SurfRad to the dup list 
                                        dupSurfRadObjectList.Add(surfRadObject);

                                        Console.WriteLine(@"found duplicate unique record {0} ", surfRadKey);

                                    }
                                    else
                                    {
                                        // since we've walked past the adjacent duplicates, aggregate them and add one record to the dup table.
                                        // will call a method to do this (sending it the dupSurfRadObjectList) 
                                        // and using a method since we'll need to process again after the last row if the last row is a dup

                                        if (currentDupCount > 0)
                                        {
                                            if (!processDupsList(dupSurfRadObjectList))
                                            {
                                                throw new System.Exception("error encountered during processing list of duplicate SurfRad records");
                                            }
                                        }


                                        currentDupCount = 0;
                                        dupSurfRadObjectList.Clear();

                                        // next, clear the dup list


                                    }

                                    lastSurfRad = surfRadKey;
                                    lastSurfRadObject = surfRadObject;


                                }
                            }
                        }
                    }
                    sqlReadConnection.Close();

                    // values needed for aggregator after work complete                   
                    DateTime etlEndTime = DateTime.Now;
                    nRecords = Counter(tableName);
                    if (!Aggregator(etlStartTime, etlEndTime, tableName, nRecords, argNoaaIncrementalDate))
                    {
                        throw new System.Exception("error encountered during Aggregator");
                    }

                }


                //
                // next ....  write the one copy of the dups into the _group table
                //

                // values needed for aggregator before work begins                   
                etlStartTime = DateTime.Now;    // reset it for the _group table  
                nRecords = 0;
                tableName = "_in_group_SurfRad";

                using (SqlConnection sqlReadConnection = new SqlConnection(strBuilderRead.ConnectionString))
                {
                    sqlReadConnection.Open();
                    using (SqlCommand sqlReadCmd = sqlReadConnection.CreateCommand())
                    {
                        sqlReadCmd.CommandText = @"SELECT 
                            noaa_year      , --  int       NULL,      -- year, i.e., 1995                                         Column0     varchar(5)      2018    
                            noaa_jday      , --  int       NULL,      -- Julian day (1 through 365 [or 366])                      Column1     varchar(4)      1  
                            noaa_month     , --  int       NULL,      -- number of the month (1-12)                               Column2     varchar(3)      1  
                            noaa_day       , --  int       NULL,      -- day of the month(1-31)                                   Column3     varchar(3)      1  
                            noaa_hour      , --  int       NULL,      -- hour of the day (0-23)                                   Column4     varchar(3)      0  
                            noaa_min       , --  int       NULL,      -- minute of the hour (0-59)                                Column5     varchar(3)      0  
                            dt             , --  real      NULL,      -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7)      0.000  
                            zen            , --  real      NULL,      -- solar zenith angle (degrees)                             Column7     varchar(7)      93.20     
                            dw_solar       , --  real      NULL,      -- downwelling global solar (Watts m^-2)                    Column8     varchar(8)      0.0 
                            dw_solar_qc    , --  int       NULL,      --                                                          Column9     varchar(2)      0     
                            uw_solar       , --  real      NULL,      -- upwelling global solar (Watts m^-2)                      Column10    varchar(8)      0.4 
                            uw_solar_qc    , --  int       NULL,      --                                                          Column11    varchar(2)      0     
                            direct_n       , --  real      NULL,      -- direct-normal solar (Watts m^-2)                         Column12    varchar(8)      0.8 
                            direct_n_qc    , --  int       NULL,      --                                                          Column13    varchar(2)      0     
                            diffuse        , --  real      NULL,      -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8)      0.7 
                            diffuse_qc     , --  int       NULL,      --                                                          Column15    varchar(2)      0   
                            dw_ir          , --  real      NULL,      -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8)      239.3 
                            dw_ir_qc       , --  int       NULL,      --                                                          Column17    varchar(2)      0   
                            dw_casetemp    , --  real      NULL,      -- downwelling IR case temp. (K)                            Column18    varchar(9)      262.88 
                            dw_casetemp_qc , --  int       NULL,      --                                                          Column19    varchar(2)      0   
                            dw_dometemp    , --  real      NULL,      -- downwelling IR dome temp. (K)                            Column20    varchar(9)      262.79 
                            dw_dometemp_qc , --  int       NULL,      --                                                          Column21    varchar(2)      0   
                            uw_ir          , --  real      NULL,      -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8)      272.9 
                            uw_ir_qc       , --  int       NULL,      --                                                          Column23    varchar(2)      0   
                            uw_casetemp    , --  real      NULL,      -- upwelling IR case temp. (K)                              Column24    varchar(9)      262.34 
                            uw_casetemp_qc , --  int       NULL,      --                                                          Column25    varchar(2)      0   
                            uw_dometemp    , --  real      NULL,      -- upwelling IR dome temp. (K)                              Column26    varchar(9)      262.23 
                            uw_dometemp_qc , --  int       NULL,      --                                                          Column27    varchar(2)      0     
                            uvb            , --  real      NULL,      -- global UVB (milliWatts m^-2)                             Column28    varchar(8)      0.0 
                            uvb_qc         , --  int       NULL,      --                                                          Column29    varchar(2)      0     
                            par            , --  real      NULL,      -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8)      0.4 
                            par_qc         , --  int       NULL,      --                                                          Column31    varchar(2)      0     
                            netsolar       , --  real      NULL,      -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8)      0.4 
                            netsolar_qc    , --  int       NULL,      --                                                          Column33    varchar(2)      0   
                            netir          , --  real      NULL,      -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8)      -33.6 
                            netir_qc       , --  int       NULL,      --                                                          Column35    varchar(2)      0   
                            totalnet       , --  real      NULL,      -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8)      -33.2 
                            totalnet_qc    , --  int       NULL,      --                                                          Column37    varchar(2)      0   
                            temp           , --  real      NULL,      -- 10-meter air temperature (?C)                            Column38    varchar(8)      -10.4 
                            temp_qc        , --  int       NULL,      --                                                          Column39    varchar(2)      0    
                            rh             , --  real      NULL,      -- relative humidity (%)                                    Column40    varchar(8)      92.1 
                            rh_qc          , --  int       NULL,      --                                                          Column41    varchar(2)      0     
                            windspd        , --  real      NULL,      -- wind speed (ms^-1)                                       Column42    varchar(8)      0.8 
                            windspd_qc     , --  int       NULL,      --                                                          Column43    varchar(2)      0   
                            winddir        , --  real      NULL,      -- wind direction (degrees, clockwise from north)           Column44    varchar(8)      110.1 
                            winddir_qc     , --  int       NULL,      --                                                          Column45    varchar(2)      0   
                            pressure       , --  real      NULL,      -- station pressure (mb)                                    Column46    varchar(8)      837.1 
                            pressure_qc    , --  int       NULL       --                                                          Column47    varchar(2)      0
                            file_date      , --  datetime  NULL,      -- noaa filedate (encoded from DayOfYear)    
                            added_date       --  datetime  NULL       -- date data added to database, often delayed
                        FROM _in_dups_SurfRad;";

                        using (SqlDataReader reader = sqlReadCmd.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {

                                while (reader.Read())
                                {
                                    var noaa_year = (reader[0] as Int32?) ?? 0;                   // int       NULL,     -- year, i.e., 1995                                         Column0     varchar(5) 
                                    var noaa_jday = (reader[1] as Int32?) ?? 0;                   // int       NULL,     -- Julian day (1 through 365 [or 366])                      Column1     varchar(4) 
                                    var noaa_month = (reader[2] as Int32?) ?? 0;                   // int       NULL,     -- number of the month (1-12)                               Column2     varchar(3) 
                                    var noaa_day = (reader[3] as Int32?) ?? 0;                   // int       NULL,     -- day of the month(1-31)                                   Column3     varchar(3) 
                                    var noaa_hour = (reader[4] as Int32?) ?? 0;                   // int       NULL,     -- hour of the day (0-23)                                   Column4     varchar(3) 
                                    var noaa_min = (reader[5] as Int32?) ?? 0;                   // int       NULL,     -- minute of the hour (0-59)                                Column5     varchar(3) 
                                    var dt = (reader[6] as Decimal?) ?? 0;                 // real      NULL,     -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7) 
                                    var zen = (reader[7] as Decimal?) ?? 0;                 // real      NULL,     -- solar zenith angle (degrees)                             Column7     varchar(7) 
                                    var dw_solar = (reader[8] as Decimal?) ?? 0;                 // real      NULL,     -- downwelling global solar (Watts m^-2)                    Column8     varchar(8) 
                                    var dw_solar_qc = (reader[9] as Int32?) ?? 0;                   // int       NULL,     --                                                          Column9     varchar(2) 
                                    var uw_solar = (reader[10] as Decimal?) ?? 0;                // real      NULL,     -- upwelling global solar (Watts m^-2)                      Column10    varchar(8) 
                                    var uw_solar_qc = (reader[11] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column11    varchar(2) 
                                    var direct_n = (reader[12] as Decimal?) ?? 0;                // real      NULL,     -- direct-normal solar (Watts m^-2)                         Column12    varchar(8) 
                                    var direct_n_qc = (reader[13] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column13    varchar(2) 
                                    var diffuse = (reader[14] as Decimal?) ?? 0;                // real      NULL,     -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8) 
                                    var diffuse_qc = (reader[15] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column15    varchar(2) 
                                    var dw_ir = (reader[16] as Decimal?) ?? 0;                // real      NULL,     -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8) 
                                    var dw_ir_qc = (reader[17] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column17    varchar(2) 
                                    var dw_casetemp = (reader[18] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR case temp. (K)                            Column18    varchar(9) 
                                    var dw_casetemp_qc = (reader[19] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column19    varchar(2) 
                                    var dw_dometemp = (reader[20] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR dome temp. (K)                            Column20    varchar(9) 
                                    var dw_dometemp_qc = (reader[21] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column21    varchar(2) 
                                    var uw_ir = (reader[22] as Decimal?) ?? 0;                // real      NULL,     -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8) 
                                    var uw_ir_qc = (reader[23] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column23    varchar(2) 
                                    var uw_casetemp = (reader[24] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR case temp. (K)                              Column24    varchar(9) 
                                    var uw_casetemp_qc = (reader[25] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column25    varchar(2) 
                                    var uw_dometemp = (reader[26] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR dome temp. (K)                              Column26    varchar(9) 
                                    var uw_dometemp_qc = (reader[27] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column27    varchar(2) 
                                    var uvb = (reader[28] as Decimal?) ?? 0;                // real      NULL,     -- global UVB (milliWatts m^-2)                             Column28    varchar(8) 
                                    var uvb_qc = (reader[29] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column29    varchar(2) 
                                    var par = (reader[30] as Decimal?) ?? 0;                // real      NULL,     -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8) 
                                    var par_qc = (reader[31] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column31    varchar(2) 
                                    var netsolar = (reader[32] as Decimal?) ?? 0;                // real      NULL,     -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8) 
                                    var netsolar_qc = (reader[33] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column33    varchar(2) 
                                    var netir = (reader[34] as Decimal?) ?? 0;                // real      NULL,     -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8) 
                                    var netir_qc = (reader[35] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column35    varchar(2) 
                                    var totalnet = (reader[36] as Decimal?) ?? 0;                // real      NULL,     -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8) 
                                    var totalnet_qc = (reader[37] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column37    varchar(2) 
                                    var temp = (reader[38] as Decimal?) ?? 0;                // real      NULL,     -- 10-meter air temperature (?C)                            Column38    varchar(8) 
                                    var temp_qc = (reader[39] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column39    varchar(2) 
                                    var rh = (reader[40] as Decimal?) ?? 0;                // real      NULL,     -- relative humidity (%)                                    Column40    varchar(8) 
                                    var rh_qc = (reader[41] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column41    varchar(2) 
                                    var windspd = (reader[42] as Decimal?) ?? 0;                // real      NULL,     -- wind speed (ms^-1)                                       Column42    varchar(8) 
                                    var windspd_qc = (reader[43] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column43    varchar(2) 
                                    var winddir = (reader[44] as Decimal?) ?? 0;                // real      NULL,     -- wind direction (degrees, clockwise from north)           Column44    varchar(8) 
                                    var winddir_qc = (reader[45] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column45    varchar(2) 
                                    var pressure = (reader[46] as Decimal?) ?? 0;                // real      NULL,     -- station pressure (mb)                                    Column46    varchar(8) 
                                    var pressure_qc = (reader[47] as Int32?) ?? 0;                  // int       NULL      --                                                          Column47    varchar(2) 
                                    var fileDate = (reader[48] as DateTime?) ?? DateTime.MinValue;
                                    var addedDate = (reader[49] as DateTime?) ?? DateTime.MinValue;


                                    using (SqlConnection sqlWriteConnection = new SqlConnection(strBuilderWrite.ConnectionString))
                                    {

                                        sqlWriteConnection.Open();
                                        using (SqlCommand sqlWriteCmd = sqlWriteConnection.CreateCommand())
                                        {
                                            sqlWriteCmd.CommandText = @"INSERT INTO _in_group_SurfRad (
                                                noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min, dt, zen, dw_solar, dw_solar_qc, uw_solar, uw_solar_qc, direct_n, direct_n_qc, diffuse, diffuse_qc, dw_ir, dw_ir_qc, dw_casetemp, dw_casetemp_qc, dw_dometemp, dw_dometemp_qc, uw_ir, uw_ir_qc, uw_casetemp, uw_casetemp_qc, uw_dometemp, uw_dometemp_qc, uvb, uvb_qc, par, par_qc, netsolar, netsolar_qc, netir, netir_qc, totalnet, totalnet_qc, temp, temp_qc, rh, rh_qc, windspd, windspd_qc, winddir, winddir_qc, pressure, pressure_qc, fileDate, addedDate
                                                ) 
                                                VALUES 
                                                (
                                                @noaa_year, @noaa_jday, @noaa_month, @noaa_day, @noaa_hour, @noaa_min, @dt, @zen, @dw_solar, @dw_solar_qc, @uw_solar, @uw_solar_qc, @direct_n, @direct_n_qc, @diffuse, @diffuse_qc, @dw_ir, @dw_ir_qc, @dw_casetemp, @dw_casetemp_qc, @dw_dometemp, @dw_dometemp_qc, @uw_ir, @uw_ir_qc, @uw_casetemp, @uw_casetemp_qc, @uw_dometemp, @uw_dometemp_qc, @uvb, @uvb_qc, @par, @par_qc, @netsolar, @netsolar_qc, @netir, @netir_qc, @totalnet, @totalnet_qc, @temp, @temp_qc, @rh, @rh_qc, @windspd, @windspd_qc, @winddir, @winddir_qc, @pressure, @pressure_qc, @fileDate, @addedDate
                                                )";

                                            sqlWriteCmd.Parameters.Add("@noaa_year      ", SqlDbType.Int, 12).Value = noaa_year;
                                            sqlWriteCmd.Parameters.Add("@noaa_jday      ", SqlDbType.Int, 12).Value = noaa_jday;
                                            sqlWriteCmd.Parameters.Add("@noaa_month     ", SqlDbType.Int, 12).Value = noaa_month;
                                            sqlWriteCmd.Parameters.Add("@noaa_day       ", SqlDbType.Int, 12).Value = noaa_day;
                                            sqlWriteCmd.Parameters.Add("@noaa_hour      ", SqlDbType.Int, 12).Value = noaa_hour;
                                            sqlWriteCmd.Parameters.Add("@noaa_min       ", SqlDbType.Int, 12).Value = noaa_min;
                                            sqlWriteCmd.Parameters.Add("@dt", SqlDbType.Decimal, 16).Value = dt;
                                            sqlWriteCmd.Parameters["@dt"].Precision = 7;
                                            sqlWriteCmd.Parameters["@dt"].Scale = 3;
                                            sqlWriteCmd.Parameters.Add("@zen", SqlDbType.Decimal, 16).Value = zen;
                                            sqlWriteCmd.Parameters["@zen"].Precision = 7;
                                            sqlWriteCmd.Parameters["@zen"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_solar", SqlDbType.Decimal, 16).Value = dw_solar;
                                            sqlWriteCmd.Parameters["@dw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_solar_qc    ", SqlDbType.Int, 12).Value = dw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_solar", SqlDbType.Decimal, 16).Value = uw_solar;
                                            sqlWriteCmd.Parameters["@uw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_solar_qc", SqlDbType.Int, 12).Value = uw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@direct_n", SqlDbType.Decimal, 16).Value = direct_n;
                                            sqlWriteCmd.Parameters["@direct_n"].Precision = 8;
                                            sqlWriteCmd.Parameters["@direct_n"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@direct_n_qc", SqlDbType.Int, 12).Value = direct_n_qc;
                                            sqlWriteCmd.Parameters.Add("@diffuse", SqlDbType.Decimal, 16).Value = diffuse;
                                            sqlWriteCmd.Parameters["@diffuse"].Precision = 8;
                                            sqlWriteCmd.Parameters["@diffuse"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@diffuse_qc", SqlDbType.Int, 12).Value = diffuse_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_ir", SqlDbType.Decimal, 16).Value = dw_ir;
                                            sqlWriteCmd.Parameters["@dw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_ir_qc", SqlDbType.Int, 12).Value = dw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp", SqlDbType.Decimal, 16).Value = dw_casetemp;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp_qc", SqlDbType.Int, 12).Value = dw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp", SqlDbType.Decimal, 16).Value = dw_dometemp;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp_qc", SqlDbType.Int, 12).Value = dw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_ir", SqlDbType.Decimal, 16).Value = uw_ir;
                                            sqlWriteCmd.Parameters["@uw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_ir_qc", SqlDbType.Int, 12).Value = uw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp", SqlDbType.Decimal, 16).Value = uw_casetemp;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp_qc", SqlDbType.Int, 12).Value = uw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp", SqlDbType.Decimal, 16).Value = uw_dometemp;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp_qc", SqlDbType.Int, 12).Value = uw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uvb", SqlDbType.Decimal, 16).Value = uvb;
                                            sqlWriteCmd.Parameters["@uvb"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uvb"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uvb_qc", SqlDbType.Int, 12).Value = uvb_qc;
                                            sqlWriteCmd.Parameters.Add("@par", SqlDbType.Decimal, 16).Value = par;
                                            sqlWriteCmd.Parameters["@par"].Precision = 8;
                                            sqlWriteCmd.Parameters["@par"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@par_qc", SqlDbType.Int, 12).Value = par_qc;
                                            sqlWriteCmd.Parameters.Add("@netsolar", SqlDbType.Decimal, 16).Value = netsolar;
                                            sqlWriteCmd.Parameters["@netsolar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netsolar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netsolar_qc", SqlDbType.Int, 12).Value = netsolar_qc;
                                            sqlWriteCmd.Parameters.Add("@netir", SqlDbType.Decimal, 16).Value = netir;
                                            sqlWriteCmd.Parameters["@netir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netir_qc", SqlDbType.Int, 12).Value = netir_qc;
                                            sqlWriteCmd.Parameters.Add("@totalnet", SqlDbType.Decimal, 16).Value = totalnet;
                                            sqlWriteCmd.Parameters["@totalnet"].Precision = 8;
                                            sqlWriteCmd.Parameters["@totalnet"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@totalnet_qc", SqlDbType.Int, 12).Value = totalnet_qc;
                                            sqlWriteCmd.Parameters.Add("@temp", SqlDbType.Decimal, 16).Value = temp;
                                            sqlWriteCmd.Parameters["@temp"].Precision = 8;
                                            sqlWriteCmd.Parameters["@temp"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@temp_qc", SqlDbType.Int, 12).Value = temp_qc;
                                            sqlWriteCmd.Parameters.Add("@rh", SqlDbType.Decimal, 16).Value = rh;
                                            sqlWriteCmd.Parameters["@rh"].Precision = 8;
                                            sqlWriteCmd.Parameters["@rh"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@rh_qc", SqlDbType.Int, 12).Value = rh_qc;
                                            sqlWriteCmd.Parameters.Add("@windspd", SqlDbType.Decimal, 16).Value = windspd;
                                            sqlWriteCmd.Parameters["@windspd"].Precision = 8;
                                            sqlWriteCmd.Parameters["@windspd"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@windspd_qc", SqlDbType.Int, 12).Value = windspd_qc;
                                            sqlWriteCmd.Parameters.Add("@winddir", SqlDbType.Decimal, 16).Value = winddir;
                                            sqlWriteCmd.Parameters["@winddir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@winddir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@winddir_qc", SqlDbType.Int, 12).Value = winddir_qc;
                                            sqlWriteCmd.Parameters.Add("@pressure", SqlDbType.Decimal, 16).Value = pressure;
                                            sqlWriteCmd.Parameters["@pressure"].Precision = 8;
                                            sqlWriteCmd.Parameters["@pressure"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@pressure_qc", SqlDbType.Int, 12).Value = pressure_qc;


                                            //Console.WriteLine(@"ready to write to _group {0} {1} {2} {3}", accountKey, DOB, age, ageAsOfDay);
                                            sqlWriteCmd.ExecuteNonQuery();

                                        }
                                        sqlWriteConnection.Close();
                                    }
                                }
                            }
                        }
                    }
                    sqlReadConnection.Close();

                }



                //
                // so ... one copy of the dups is written into the _group table
                // next ....  write the non-duped records from _clean into the _group table
                //

                using (SqlConnection sqlReadConnection = new SqlConnection(strBuilderRead.ConnectionString))
                {
                    sqlReadConnection.Open();
                    using (SqlCommand sqlReadCmd = sqlReadConnection.CreateCommand())
                    {
                        sqlReadCmd.CommandText = @"SELECT 
                            noaa_year      , --  int       NULL,      -- year, i.e., 1995                                         Column0     varchar(5)      2018    
                            noaa_jday      , --  int       NULL,      -- Julian day (1 through 365 [or 366])                      Column1     varchar(4)      1  
                            noaa_month     , --  int       NULL,      -- number of the month (1-12)                               Column2     varchar(3)      1  
                            noaa_day       , --  int       NULL,      -- day of the month(1-31)                                   Column3     varchar(3)      1  
                            noaa_hour      , --  int       NULL,      -- hour of the day (0-23)                                   Column4     varchar(3)      0  
                            noaa_min       , --  int       NULL,      -- minute of the hour (0-59)                                Column5     varchar(3)      0  
                            dt             , --  real      NULL,      -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7)      0.000  
                            zen            , --  real      NULL,      -- solar zenith angle (degrees)                             Column7     varchar(7)      93.20     
                            dw_solar       , --  real      NULL,      -- downwelling global solar (Watts m^-2)                    Column8     varchar(8)      0.0 
                            dw_solar_qc    , --  int       NULL,      --                                                          Column9     varchar(2)      0     
                            uw_solar       , --  real      NULL,      -- upwelling global solar (Watts m^-2)                      Column10    varchar(8)      0.4 
                            uw_solar_qc    , --  int       NULL,      --                                                          Column11    varchar(2)      0     
                            direct_n       , --  real      NULL,      -- direct-normal solar (Watts m^-2)                         Column12    varchar(8)      0.8 
                            direct_n_qc    , --  int       NULL,      --                                                          Column13    varchar(2)      0     
                            diffuse        , --  real      NULL,      -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8)      0.7 
                            diffuse_qc     , --  int       NULL,      --                                                          Column15    varchar(2)      0   
                            dw_ir          , --  real      NULL,      -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8)      239.3 
                            dw_ir_qc       , --  int       NULL,      --                                                          Column17    varchar(2)      0   
                            dw_casetemp    , --  real      NULL,      -- downwelling IR case temp. (K)                            Column18    varchar(9)      262.88 
                            dw_casetemp_qc , --  int       NULL,      --                                                          Column19    varchar(2)      0   
                            dw_dometemp    , --  real      NULL,      -- downwelling IR dome temp. (K)                            Column20    varchar(9)      262.79 
                            dw_dometemp_qc , --  int       NULL,      --                                                          Column21    varchar(2)      0   
                            uw_ir          , --  real      NULL,      -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8)      272.9 
                            uw_ir_qc       , --  int       NULL,      --                                                          Column23    varchar(2)      0   
                            uw_casetemp    , --  real      NULL,      -- upwelling IR case temp. (K)                              Column24    varchar(9)      262.34 
                            uw_casetemp_qc , --  int       NULL,      --                                                          Column25    varchar(2)      0   
                            uw_dometemp    , --  real      NULL,      -- upwelling IR dome temp. (K)                              Column26    varchar(9)      262.23 
                            uw_dometemp_qc , --  int       NULL,      --                                                          Column27    varchar(2)      0     
                            uvb            , --  real      NULL,      -- global UVB (milliWatts m^-2)                             Column28    varchar(8)      0.0 
                            uvb_qc         , --  int       NULL,      --                                                          Column29    varchar(2)      0     
                            par            , --  real      NULL,      -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8)      0.4 
                            par_qc         , --  int       NULL,      --                                                          Column31    varchar(2)      0     
                            netsolar       , --  real      NULL,      -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8)      0.4 
                            netsolar_qc    , --  int       NULL,      --                                                          Column33    varchar(2)      0   
                            netir          , --  real      NULL,      -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8)      -33.6 
                            netir_qc       , --  int       NULL,      --                                                          Column35    varchar(2)      0   
                            totalnet       , --  real      NULL,      -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8)      -33.2 
                            totalnet_qc    , --  int       NULL,      --                                                          Column37    varchar(2)      0   
                            temp           , --  real      NULL,      -- 10-meter air temperature (?C)                            Column38    varchar(8)      -10.4 
                            temp_qc        , --  int       NULL,      --                                                          Column39    varchar(2)      0    
                            rh             , --  real      NULL,      -- relative humidity (%)                                    Column40    varchar(8)      92.1 
                            rh_qc          , --  int       NULL,      --                                                          Column41    varchar(2)      0     
                            windspd        , --  real      NULL,      -- wind speed (ms^-1)                                       Column42    varchar(8)      0.8 
                            windspd_qc     , --  int       NULL,      --                                                          Column43    varchar(2)      0   
                            winddir        , --  real      NULL,      -- wind direction (degrees, clockwise from north)           Column44    varchar(8)      110.1 
                            winddir_qc     , --  int       NULL,      --                                                          Column45    varchar(2)      0   
                            pressure       , --  real      NULL,      -- station pressure (mb)                                    Column46    varchar(8)      837.1 
                            pressure_qc    , --  int       NULL       --                                                          Column47    varchar(2)      0
                            file_date      , --  datetime  NULL,      -- noaa filedate (encoded from DayOfYear)    
                            added_date       --  datetime  NULL       -- date data added to database, often delayed
                        FROM _in_clean_SurfRad
                        WHERE 
                            (noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min) NOT IN (SELECT (noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min) FROM _in_dups_SurfRad);";

                        using (SqlDataReader reader = sqlReadCmd.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                int i = 0;

                                while (reader.Read())
                                {
                                    i++;

                                    var noaa_year = (reader[0] as Int32?) ?? 0;                   // int       NULL,     -- year, i.e., 1995                                         Column0     varchar(5) 
                                    var noaa_jday = (reader[1] as Int32?) ?? 0;                   // int       NULL,     -- Julian day (1 through 365 [or 366])                      Column1     varchar(4) 
                                    var noaa_month = (reader[2] as Int32?) ?? 0;                   // int       NULL,     -- number of the month (1-12)                               Column2     varchar(3) 
                                    var noaa_day = (reader[3] as Int32?) ?? 0;                   // int       NULL,     -- day of the month(1-31)                                   Column3     varchar(3) 
                                    var noaa_hour = (reader[4] as Int32?) ?? 0;                   // int       NULL,     -- hour of the day (0-23)                                   Column4     varchar(3) 
                                    var noaa_min = (reader[5] as Int32?) ?? 0;                   // int       NULL,     -- minute of the hour (0-59)                                Column5     varchar(3) 
                                    var dt = (reader[6] as Decimal?) ?? 0;                 // real      NULL,     -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7) 
                                    var zen = (reader[7] as Decimal?) ?? 0;                 // real      NULL,     -- solar zenith angle (degrees)                             Column7     varchar(7) 
                                    var dw_solar = (reader[8] as Decimal?) ?? 0;                 // real      NULL,     -- downwelling global solar (Watts m^-2)                    Column8     varchar(8) 
                                    var dw_solar_qc = (reader[9] as Int32?) ?? 0;                   // int       NULL,     --                                                          Column9     varchar(2) 
                                    var uw_solar = (reader[10] as Decimal?) ?? 0;                // real      NULL,     -- upwelling global solar (Watts m^-2)                      Column10    varchar(8) 
                                    var uw_solar_qc = (reader[11] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column11    varchar(2) 
                                    var direct_n = (reader[12] as Decimal?) ?? 0;                // real      NULL,     -- direct-normal solar (Watts m^-2)                         Column12    varchar(8) 
                                    var direct_n_qc = (reader[13] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column13    varchar(2) 
                                    var diffuse = (reader[14] as Decimal?) ?? 0;                // real      NULL,     -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8) 
                                    var diffuse_qc = (reader[15] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column15    varchar(2) 
                                    var dw_ir = (reader[16] as Decimal?) ?? 0;                // real      NULL,     -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8) 
                                    var dw_ir_qc = (reader[17] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column17    varchar(2) 
                                    var dw_casetemp = (reader[18] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR case temp. (K)                            Column18    varchar(9) 
                                    var dw_casetemp_qc = (reader[19] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column19    varchar(2) 
                                    var dw_dometemp = (reader[20] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR dome temp. (K)                            Column20    varchar(9) 
                                    var dw_dometemp_qc = (reader[21] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column21    varchar(2) 
                                    var uw_ir = (reader[22] as Decimal?) ?? 0;                // real      NULL,     -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8) 
                                    var uw_ir_qc = (reader[23] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column23    varchar(2) 
                                    var uw_casetemp = (reader[24] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR case temp. (K)                              Column24    varchar(9) 
                                    var uw_casetemp_qc = (reader[25] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column25    varchar(2) 
                                    var uw_dometemp = (reader[26] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR dome temp. (K)                              Column26    varchar(9) 
                                    var uw_dometemp_qc = (reader[27] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column27    varchar(2) 
                                    var uvb = (reader[28] as Decimal?) ?? 0;                // real      NULL,     -- global UVB (milliWatts m^-2)                             Column28    varchar(8) 
                                    var uvb_qc = (reader[29] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column29    varchar(2) 
                                    var par = (reader[30] as Decimal?) ?? 0;                // real      NULL,     -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8) 
                                    var par_qc = (reader[31] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column31    varchar(2) 
                                    var netsolar = (reader[32] as Decimal?) ?? 0;                // real      NULL,     -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8) 
                                    var netsolar_qc = (reader[33] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column33    varchar(2) 
                                    var netir = (reader[34] as Decimal?) ?? 0;                // real      NULL,     -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8) 
                                    var netir_qc = (reader[35] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column35    varchar(2) 
                                    var totalnet = (reader[36] as Decimal?) ?? 0;                // real      NULL,     -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8) 
                                    var totalnet_qc = (reader[37] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column37    varchar(2) 
                                    var temp = (reader[38] as Decimal?) ?? 0;                // real      NULL,     -- 10-meter air temperature (?C)                            Column38    varchar(8) 
                                    var temp_qc = (reader[39] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column39    varchar(2) 
                                    var rh = (reader[40] as Decimal?) ?? 0;                // real      NULL,     -- relative humidity (%)                                    Column40    varchar(8) 
                                    var rh_qc = (reader[41] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column41    varchar(2) 
                                    var windspd = (reader[42] as Decimal?) ?? 0;                // real      NULL,     -- wind speed (ms^-1)                                       Column42    varchar(8) 
                                    var windspd_qc = (reader[43] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column43    varchar(2) 
                                    var winddir = (reader[44] as Decimal?) ?? 0;                // real      NULL,     -- wind direction (degrees, clockwise from north)           Column44    varchar(8) 
                                    var winddir_qc = (reader[45] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column45    varchar(2) 
                                    var pressure = (reader[46] as Decimal?) ?? 0;                // real      NULL,     -- station pressure (mb)                                    Column46    varchar(8) 
                                    var pressure_qc = (reader[47] as Int32?) ?? 0;                  // int       NULL      --                                                          Column47    varchar(2) 
                                    var fileDate = (reader[48] as DateTime?) ?? DateTime.MinValue;
                                    var addedDate = (reader[49] as DateTime?) ?? DateTime.MinValue;

                                    using (SqlConnection sqlWriteConnection = new SqlConnection(strBuilderWrite.ConnectionString))
                                    {

                                        sqlWriteConnection.Open();
                                        using (SqlCommand sqlWriteCmd = sqlWriteConnection.CreateCommand())
                                        {
                                            sqlWriteCmd.CommandText = @"INSERT INTO _in_group_SurfRad (
                                                noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min, dt, zen, dw_solar, dw_solar_qc, uw_solar, uw_solar_qc, direct_n, direct_n_qc, diffuse, diffuse_qc, dw_ir, dw_ir_qc, dw_casetemp, dw_casetemp_qc, dw_dometemp, dw_dometemp_qc, uw_ir, uw_ir_qc, uw_casetemp, uw_casetemp_qc, uw_dometemp, uw_dometemp_qc, uvb, uvb_qc, par, par_qc, netsolar, netsolar_qc, netir, netir_qc, totalnet, totalnet_qc, temp, temp_qc, rh, rh_qc, windspd, windspd_qc, winddir, winddir_qc, pressure, pressure_qc, fileDate, addedDate
                                                ) 
                                                VALUES 
                                                (
                                                @noaa_year, @noaa_jday, @noaa_month, @noaa_day, @noaa_hour, @noaa_min, @dt, @zen, @dw_solar, @dw_solar_qc, @uw_solar, @uw_solar_qc, @direct_n, @direct_n_qc, @diffuse, @diffuse_qc, @dw_ir, @dw_ir_qc, @dw_casetemp, @dw_casetemp_qc, @dw_dometemp, @dw_dometemp_qc, @uw_ir, @uw_ir_qc, @uw_casetemp, @uw_casetemp_qc, @uw_dometemp, @uw_dometemp_qc, @uvb, @uvb_qc, @par, @par_qc, @netsolar, @netsolar_qc, @netir, @netir_qc, @totalnet, @totalnet_qc, @temp, @temp_qc, @rh, @rh_qc, @windspd, @windspd_qc, @winddir, @winddir_qc, @pressure, @pressure_qc, @fileDate, @addedDate
                                                )";

                                            sqlWriteCmd.Parameters.Add("@noaa_year      ", SqlDbType.Int, 12).Value = noaa_year;
                                            sqlWriteCmd.Parameters.Add("@noaa_jday      ", SqlDbType.Int, 12).Value = noaa_jday;
                                            sqlWriteCmd.Parameters.Add("@noaa_month     ", SqlDbType.Int, 12).Value = noaa_month;
                                            sqlWriteCmd.Parameters.Add("@noaa_day       ", SqlDbType.Int, 12).Value = noaa_day;
                                            sqlWriteCmd.Parameters.Add("@noaa_hour      ", SqlDbType.Int, 12).Value = noaa_hour;
                                            sqlWriteCmd.Parameters.Add("@noaa_min       ", SqlDbType.Int, 12).Value = noaa_min;
                                            sqlWriteCmd.Parameters.Add("@dt", SqlDbType.Decimal, 16).Value = dt;
                                            sqlWriteCmd.Parameters["@dt"].Precision = 7;
                                            sqlWriteCmd.Parameters["@dt"].Scale = 3;
                                            sqlWriteCmd.Parameters.Add("@zen", SqlDbType.Decimal, 16).Value = zen;
                                            sqlWriteCmd.Parameters["@zen"].Precision = 7;
                                            sqlWriteCmd.Parameters["@zen"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_solar", SqlDbType.Decimal, 16).Value = dw_solar;
                                            sqlWriteCmd.Parameters["@dw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_solar_qc    ", SqlDbType.Int, 12).Value = dw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_solar", SqlDbType.Decimal, 16).Value = uw_solar;
                                            sqlWriteCmd.Parameters["@uw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_solar_qc", SqlDbType.Int, 12).Value = uw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@direct_n", SqlDbType.Decimal, 16).Value = direct_n;
                                            sqlWriteCmd.Parameters["@direct_n"].Precision = 8;
                                            sqlWriteCmd.Parameters["@direct_n"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@direct_n_qc", SqlDbType.Int, 12).Value = direct_n_qc;
                                            sqlWriteCmd.Parameters.Add("@diffuse", SqlDbType.Decimal, 16).Value = diffuse;
                                            sqlWriteCmd.Parameters["@diffuse"].Precision = 8;
                                            sqlWriteCmd.Parameters["@diffuse"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@diffuse_qc", SqlDbType.Int, 12).Value = diffuse_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_ir", SqlDbType.Decimal, 16).Value = dw_ir;
                                            sqlWriteCmd.Parameters["@dw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_ir_qc", SqlDbType.Int, 12).Value = dw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp", SqlDbType.Decimal, 16).Value = dw_casetemp;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp_qc", SqlDbType.Int, 12).Value = dw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp", SqlDbType.Decimal, 16).Value = dw_dometemp;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp_qc", SqlDbType.Int, 12).Value = dw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_ir", SqlDbType.Decimal, 16).Value = uw_ir;
                                            sqlWriteCmd.Parameters["@uw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_ir_qc", SqlDbType.Int, 12).Value = uw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp", SqlDbType.Decimal, 16).Value = uw_casetemp;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp_qc", SqlDbType.Int, 12).Value = uw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp", SqlDbType.Decimal, 16).Value = uw_dometemp;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp_qc", SqlDbType.Int, 12).Value = uw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uvb", SqlDbType.Decimal, 16).Value = uvb;
                                            sqlWriteCmd.Parameters["@uvb"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uvb"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uvb_qc", SqlDbType.Int, 12).Value = uvb_qc;
                                            sqlWriteCmd.Parameters.Add("@par", SqlDbType.Decimal, 16).Value = par;
                                            sqlWriteCmd.Parameters["@par"].Precision = 8;
                                            sqlWriteCmd.Parameters["@par"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@par_qc", SqlDbType.Int, 12).Value = par_qc;
                                            sqlWriteCmd.Parameters.Add("@netsolar", SqlDbType.Decimal, 16).Value = netsolar;
                                            sqlWriteCmd.Parameters["@netsolar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netsolar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netsolar_qc", SqlDbType.Int, 12).Value = netsolar_qc;
                                            sqlWriteCmd.Parameters.Add("@netir", SqlDbType.Decimal, 16).Value = netir;
                                            sqlWriteCmd.Parameters["@netir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netir_qc", SqlDbType.Int, 12).Value = netir_qc;
                                            sqlWriteCmd.Parameters.Add("@totalnet", SqlDbType.Decimal, 16).Value = totalnet;
                                            sqlWriteCmd.Parameters["@totalnet"].Precision = 8;
                                            sqlWriteCmd.Parameters["@totalnet"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@totalnet_qc", SqlDbType.Int, 12).Value = totalnet_qc;
                                            sqlWriteCmd.Parameters.Add("@temp", SqlDbType.Decimal, 16).Value = temp;
                                            sqlWriteCmd.Parameters["@temp"].Precision = 8;
                                            sqlWriteCmd.Parameters["@temp"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@temp_qc", SqlDbType.Int, 12).Value = temp_qc;
                                            sqlWriteCmd.Parameters.Add("@rh", SqlDbType.Decimal, 16).Value = rh;
                                            sqlWriteCmd.Parameters["@rh"].Precision = 8;
                                            sqlWriteCmd.Parameters["@rh"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@rh_qc", SqlDbType.Int, 12).Value = rh_qc;
                                            sqlWriteCmd.Parameters.Add("@windspd", SqlDbType.Decimal, 16).Value = windspd;
                                            sqlWriteCmd.Parameters["@windspd"].Precision = 8;
                                            sqlWriteCmd.Parameters["@windspd"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@windspd_qc", SqlDbType.Int, 12).Value = windspd_qc;
                                            sqlWriteCmd.Parameters.Add("@winddir", SqlDbType.Decimal, 16).Value = winddir;
                                            sqlWriteCmd.Parameters["@winddir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@winddir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@winddir_qc", SqlDbType.Int, 12).Value = winddir_qc;
                                            sqlWriteCmd.Parameters.Add("@pressure", SqlDbType.Decimal, 16).Value = pressure;
                                            sqlWriteCmd.Parameters["@pressure"].Precision = 8;
                                            sqlWriteCmd.Parameters["@pressure"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@pressure_qc", SqlDbType.Int, 12).Value = pressure_qc;

                                            //Console.WriteLine(@"{0}: ready to write to _group {1} {2} {3} {4}", i, accountKey, DOB, age, ageAsOfDay);
                                            sqlWriteCmd.ExecuteNonQuery();

                                        }
                                        sqlWriteConnection.Close();
                                    }
                                }
                            }
                        }
                    }
                    sqlReadConnection.Close();

                    // values needed for aggregator after work complete                   
                    DateTime etlEndTime = DateTime.Now;
                    nRecords = Counter(tableName);
                    if (!Aggregator(etlStartTime, etlEndTime, tableName, nRecords, argNoaaIncrementalDate))
                    {
                        throw new System.Exception("error encountered during Aggregator");
                    }
                }

            }
            catch (System.Exception e)
            {
                Console.WriteLine("    Caught an exception: " + e.Source + " " + e.Message + " " + e.StackTrace);
                return false;
            }
            return true;
        }

        /// <summary>
        /// processDupsList() method will: 
        ///     loop over the dupSurfRadObjectList with linq
        ///     keep a running total of the max and min of each field for the whole set
        ///     open a database connection to a dup table and write the key, and all the max and min values, and the final selected value 
        /// -- final selected value will likely be programmatically choosing the record with the max values especially if there is a date to use (not always the case where there is a date) 
        /// </summary>
        static bool processDupsList(List<Dictionary<string, object>> dupsList)
        {
            //Console.WriteLine(@"processing {0} duplicates", dupsList.Count());
            try
            {
                var lastDup = dupsList.Last();  // to keep logic simple, choose the last record ... 


                // https://stackoverflow.com/questions/1267080/how-to-find-the-maximum-value-for-each-key-in-a-list-of-dictionaries-using-linq
                // results list will have an entry for each field in the dictionary, each entry will have fieldname, max and min values for all the dups, and a boolean if all values same for the field
                var results = dupsList.SelectMany(d => d)
                  .GroupBy(d => d.Key)
                  .Select(g => new
                  {
                      FieldName = g.Key,
                      MaxValue = g.Max(i => i.Value).ToString(),
                      MinValue = g.Min(i => i.Value).ToString(),
                      SameValue = (g.Max(i => i.Value) == g.Min(i => i.Value))
                  });

                //if (dupsList.Count() > 2)
                //{
                //    Console.WriteLine(@"high number of dups:   count={0}     <---------------- {1}", dupsList.Count(), string.Join(";", lastDup.Values));
                //}
                //foreach (var item in results)
                //{
                //    if (!item.SameValue)
                //    {
                //        Console.WriteLine(@"    {0} {1} {2} {3} ", item.FieldName, item.MinValue, item.MaxValue, item.SameValue);
                //        Console.WriteLine(@"    ");
                //    }
                //}

                SqlConnectionStringBuilder strBuilderWrite = new SqlConnectionStringBuilder();
                strBuilderWrite.DataSource = "127.0.0.1,31433";
                strBuilderWrite.InitialCatalog = "noaaC";
                //strBuilderRead.IntegratedSecurity = true;
                strBuilderWrite.UserID = "sa";
                strBuilderWrite.Password = "omg.passwords!";
                strBuilderWrite.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;

                using (SqlConnection sqlWriteConnection = new SqlConnection(strBuilderWrite.ConnectionString))
                {
                    sqlWriteConnection.Open();
                    using (SqlCommand sqlWriteCmd = sqlWriteConnection.CreateCommand())
                    {
                        sqlWriteCmd.CommandText = @"INSERT INTO _in_dups_SurfRad (
                            noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min, dt, min_dt, max_dt, zen, min_zen, max_zen, dw_solar, min_dw_solar, max_dw_solar, dw_solar_qc, uw_solar, min_uw_solar, max_uw_solar, uw_solar_qc, direct_n, min_direct_n, max_direct_n, direct_n_qc, diffuse, min_diffuse, max_diffuse, diffuse_qc, dw_ir, min_dw_ir, max_dw_ir, dw_ir_qc, dw_casetemp, min_dw_casetemp, max_dw_casetemp, dw_casetemp_qc, dw_dometemp, min_dw_dometemp, max_dw_dometemp, dw_dometemp_qc, uw_ir, min_uw_ir, max_uw_ir, uw_ir_qc, uw_casetemp, min_uw_casetemp, max_uw_casetemp, uw_casetemp_qc, uw_dometemp, min_uw_dometemp, max_uw_dometemp, uw_dometemp_qc, uvb, min_uvb, max_uvb, uvb_qc, par, min_par, max_par, par_qc, netsolar, min_netsolar, max_netsolar, netsolar_qc, netir, min_netir, max_netir, netir_qc, totalnet, min_totalnet, max_totalnet, totalnet_qc, temp, min_temp, max_temp, temp_qc, rh, min_rh, max_rh, rh_qc, windspd, min_windspd, max_windspd, windspd_qc, winddir, min_winddir, max_winddir, winddir_qc, pressure, min_pressure, max_pressure, pressure_qc, fileDate, addedDate
                            ) 
                        VALUES 
                            (
                            @noaa_year, @noaa_jday, @noaa_month, @noaa_day, @noaa_hour, @noaa_min, @dt, @min_dt, @max_dt, @zen, @min_zen, @max_zen, @dw_solar, @min_dw_solar, @max_dw_solar, @dw_solar_qc, @uw_solar, @min_uw_solar, @max_uw_solar, @uw_solar_qc, @direct_n, @min_direct_n, @max_direct_n, @direct_n_qc, @diffuse, @min_diffuse, @max_diffuse, @diffuse_qc, @dw_ir, @min_dw_ir, @max_dw_ir, @dw_ir_qc, @dw_casetemp, @min_dw_casetemp, @max_dw_casetemp, @dw_casetemp_qc, @dw_dometemp, @min_dw_dometemp, @max_dw_dometemp, @dw_dometemp_qc, @uw_ir, @min_uw_ir, @max_uw_ir, @uw_ir_qc, @uw_casetemp, @min_uw_casetemp, @max_uw_casetemp, @uw_casetemp_qc, @uw_dometemp, @min_uw_dometemp, @max_uw_dometemp, @uw_dometemp_qc, @uvb, @min_uvb, @max_uvb, @uvb_qc, @par, @min_par, @max_par, @par_qc, @netsolar, @min_netsolar, @max_netsolar, @netsolar_qc, @netir, @min_netir, @max_netir, @netir_qc, @totalnet, @min_totalnet, @max_totalnet, @totalnet_qc, @temp, @min_temp, @max_temp, @temp_qc, @rh, @min_rh, @max_rh, @rh_qc, @windspd, @min_windspd, @max_windspd, @windspd_qc, @winddir, @min_winddir, @max_winddir, @winddir_qc, @pressure, @min_pressure, @max_pressure, @pressure_qc, @fileDate, @addedDate
                            )";

                        //for debugging:  spare list of all fields including max and min that goes above 
                        //for debugging:  spare list of all @parameters including max and min that goes above 

                        // if (!dic.TryGetValue(key, out value item)) item = dic[key] = new Item();

                        // pull the values out of the surfRadObject dictionary, specifically the last surfRadObject in the list (in lastDup) 
                        // use linq for max and min values in the results "dups" dictionary

                        // -- linq to string  
                        //object objfoo = lastDup.TryGetValue("foo", out objfoo) ? objfoo.ToString() : "";
                        //object objmin_foo = string.Join(";", (from item in results where item.FieldName == "foo" & item.SameValue == false select item.MinValue));
                        //object objmax_foo = string.Join(";", (from item in results where item.FieldName == "foo" & item.SameValue == false select item.MaxValue));
                        // -- linq to date
                        //object objfoodate = lastDup.TryGetValue("foodate", out objfoodate) ? objfoodate.ToString() : "";
                        //object objmin_foodate = results.ElementAt(3).MinValue;
                        //object objmax_foodate = results.ElementAt(3).MaxValue;
                        //-- linq to integer
                        //object objfoo = lastDup.TryGetValue("foo", out objfoo) ? objfoo.ToString() : "";
                        //object objmin_foo = results.ElementAt(6).MinValue;
                        //object objmax_foo = results.ElementAt(6).MaxValue;

                        // no min and max for the unique key   noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min
                        object objnoaa_year = lastDup.TryGetValue("noaa_year", out objnoaa_year) ? objnoaa_year.ToString() : "";
                        object objnoaa_jday = lastDup.TryGetValue("noaa_jday", out objnoaa_jday) ? objnoaa_jday.ToString() : "";
                        object objnoaa_month = lastDup.TryGetValue("noaa_month", out objnoaa_month) ? objnoaa_month.ToString() : "";
                        object objnoaa_day = lastDup.TryGetValue("noaa_day", out objnoaa_day) ? objnoaa_day.ToString() : "";
                        object objnoaa_hour = lastDup.TryGetValue("noaa_hour", out objnoaa_hour) ? objnoaa_hour.ToString() : "";
                        object objnoaa_min = lastDup.TryGetValue("noaa_min", out objnoaa_min) ? objnoaa_min.ToString() : "";
                        // min and max for the non key fields
                        object objdt = lastDup.TryGetValue("dt", out objdt) ? objdt.ToString() : "";
                        object objmin_dt = results.ElementAt(7).MinValue;
                        object objmax_dt = results.ElementAt(7).MaxValue;
                        object objzen = lastDup.TryGetValue("zen", out objzen) ? objzen.ToString() : "";
                        object objmin_zen = results.ElementAt(8).MinValue;
                        object objmax_zen = results.ElementAt(8).MaxValue;
                        object objdw_solar = lastDup.TryGetValue("dw_solar", out objdw_solar) ? objdw_solar.ToString() : "";
                        object objmin_dw_solar = results.ElementAt(9).MinValue;
                        object objmax_dw_solar = results.ElementAt(9).MaxValue;
                        object objdw_solar_qc = lastDup.TryGetValue("dw_solar_qc", out objdw_solar_qc) ? objdw_solar_qc.ToString() : "";
                        object objuw_solar = lastDup.TryGetValue("uw_solar", out objuw_solar) ? objuw_solar.ToString() : "";
                        object objmin_uw_solar = results.ElementAt(11).MinValue;
                        object objmax_uw_solar = results.ElementAt(11).MaxValue;
                        object objuw_solar_qc = lastDup.TryGetValue("uw_solar_qc", out objuw_solar_qc) ? objuw_solar_qc.ToString() : "";
                        object objdirect_n = lastDup.TryGetValue("direct_n", out objdirect_n) ? objdirect_n.ToString() : "";
                        object objmin_direct_n = results.ElementAt(13).MinValue;
                        object objmax_direct_n = results.ElementAt(13).MaxValue;
                        object objdirect_n_qc = lastDup.TryGetValue("direct_n_qc", out objdirect_n_qc) ? objdirect_n_qc.ToString() : "";
                        object objdiffuse = lastDup.TryGetValue("diffuse", out objdiffuse) ? objdiffuse.ToString() : "";
                        object objmin_diffuse = results.ElementAt(15).MinValue;
                        object objmax_diffuse = results.ElementAt(15).MaxValue;
                        object objdiffuse_qc = lastDup.TryGetValue("diffuse_qc", out objdiffuse_qc) ? objdiffuse_qc.ToString() : "";
                        object objdw_ir = lastDup.TryGetValue("dw_ir", out objdw_ir) ? objdw_ir.ToString() : "";
                        object objmin_dw_ir = results.ElementAt(17).MinValue;
                        object objmax_dw_ir = results.ElementAt(17).MaxValue;
                        object objdw_ir_qc = lastDup.TryGetValue("dw_ir_qc", out objdw_ir_qc) ? objdw_ir_qc.ToString() : "";
                        object objdw_casetemp = lastDup.TryGetValue("dw_casetemp", out objdw_casetemp) ? objdw_casetemp.ToString() : "";
                        object objmin_dw_casetemp = results.ElementAt(19).MinValue;
                        object objmax_dw_casetemp = results.ElementAt(19).MaxValue;
                        object objdw_casetemp_qc = lastDup.TryGetValue("dw_casetemp_qc", out objdw_casetemp_qc) ? objdw_casetemp_qc.ToString() : "";
                        object objdw_dometemp = lastDup.TryGetValue("dw_dometemp", out objdw_dometemp) ? objdw_dometemp.ToString() : "";
                        object objmin_dw_dometemp = results.ElementAt(21).MinValue;
                        object objmax_dw_dometemp = results.ElementAt(21).MaxValue;
                        object objdw_dometemp_qc = lastDup.TryGetValue("dw_dometemp_qc", out objdw_dometemp_qc) ? objdw_dometemp_qc.ToString() : "";
                        object objuw_ir = lastDup.TryGetValue("uw_ir", out objuw_ir) ? objuw_ir.ToString() : "";
                        object objmin_uw_ir = results.ElementAt(23).MinValue;
                        object objmax_uw_ir = results.ElementAt(23).MaxValue;
                        object objuw_ir_qc = lastDup.TryGetValue("uw_ir_qc", out objuw_ir_qc) ? objuw_ir_qc.ToString() : "";
                        object objuw_casetemp = lastDup.TryGetValue("uw_casetemp", out objuw_casetemp) ? objuw_casetemp.ToString() : "";
                        object objmin_uw_casetemp = results.ElementAt(25).MinValue;
                        object objmax_uw_casetemp = results.ElementAt(25).MaxValue;
                        object objuw_casetemp_qc = lastDup.TryGetValue("uw_casetemp_qc", out objuw_casetemp_qc) ? objuw_casetemp_qc.ToString() : "";
                        object objuw_dometemp = lastDup.TryGetValue("uw_dometemp", out objuw_dometemp) ? objuw_dometemp.ToString() : "";
                        object objmin_uw_dometemp = results.ElementAt(27).MinValue;
                        object objmax_uw_dometemp = results.ElementAt(27).MaxValue;
                        object objuw_dometemp_qc = lastDup.TryGetValue("uw_dometemp_qc", out objuw_dometemp_qc) ? objuw_dometemp_qc.ToString() : "";
                        object objuvb = lastDup.TryGetValue("uvb", out objuvb) ? objuvb.ToString() : "";
                        object objmin_uvb = results.ElementAt(29).MinValue;
                        object objmax_uvb = results.ElementAt(29).MaxValue;
                        object objuvb_qc = lastDup.TryGetValue("uvb_qc", out objuvb_qc) ? objuvb_qc.ToString() : "";
                        object objpar = lastDup.TryGetValue("par", out objpar) ? objpar.ToString() : "";
                        object objmin_par = results.ElementAt(31).MinValue;
                        object objmax_par = results.ElementAt(31).MaxValue;
                        object objpar_qc = lastDup.TryGetValue("par_qc", out objpar_qc) ? objpar_qc.ToString() : "";
                        object objnetsolar = lastDup.TryGetValue("netsolar", out objnetsolar) ? objnetsolar.ToString() : "";
                        object objmin_netsolar = results.ElementAt(33).MinValue;
                        object objmax_netsolar = results.ElementAt(33).MaxValue;
                        object objnetsolar_qc = lastDup.TryGetValue("netsolar_qc", out objnetsolar_qc) ? objnetsolar_qc.ToString() : "";
                        object objnetir = lastDup.TryGetValue("netir", out objnetir) ? objnetir.ToString() : "";
                        object objmin_netir = results.ElementAt(35).MinValue;
                        object objmax_netir = results.ElementAt(35).MaxValue;
                        object objnetir_qc = lastDup.TryGetValue("netir_qc", out objnetir_qc) ? objnetir_qc.ToString() : "";
                        object objtotalnet = lastDup.TryGetValue("totalnet", out objtotalnet) ? objtotalnet.ToString() : "";
                        object objmin_totalnet = results.ElementAt(37).MinValue;
                        object objmax_totalnet = results.ElementAt(37).MaxValue;
                        object objtotalnet_qc = lastDup.TryGetValue("totalnet_qc", out objtotalnet_qc) ? objtotalnet_qc.ToString() : "";
                        object objtemp = lastDup.TryGetValue("temp", out objtemp) ? objtemp.ToString() : "";
                        object objmin_temp = results.ElementAt(39).MinValue;
                        object objmax_temp = results.ElementAt(39).MaxValue;
                        object objtemp_qc = lastDup.TryGetValue("temp_qc", out objtemp_qc) ? objtemp_qc.ToString() : "";
                        object objrh = lastDup.TryGetValue("rh", out objrh) ? objrh.ToString() : "";
                        object objmin_rh = results.ElementAt(41).MinValue;
                        object objmax_rh = results.ElementAt(41).MaxValue;
                        object objrh_qc = lastDup.TryGetValue("rh_qc", out objrh_qc) ? objrh_qc.ToString() : "";
                        object objwindspd = lastDup.TryGetValue("windspd", out objwindspd) ? objwindspd.ToString() : "";
                        object objmin_windspd = results.ElementAt(43).MinValue;
                        object objmax_windspd = results.ElementAt(43).MaxValue;
                        object objwindspd_qc = lastDup.TryGetValue("windspd_qc", out objwindspd_qc) ? objwindspd_qc.ToString() : "";
                        object objwinddir = lastDup.TryGetValue("winddir", out objwinddir) ? objwinddir.ToString() : "";
                        object objmin_winddir = results.ElementAt(45).MinValue;
                        object objmax_winddir = results.ElementAt(45).MaxValue;
                        object objwinddir_qc = lastDup.TryGetValue("winddir_qc", out objwinddir_qc) ? objwinddir_qc.ToString() : "";
                        object objpressure = lastDup.TryGetValue("pressure", out objpressure) ? objpressure.ToString() : "";
                        object objmin_pressure = results.ElementAt(47).MinValue;
                        object objmax_pressure = results.ElementAt(47).MaxValue;
                        object objpressure_qc = lastDup.TryGetValue("pressure_qc", out objpressure_qc) ? objpressure_qc.ToString() : "";
                        // no min and max for the internal variables:   fileDate, addedDate
                        object objfileDate = lastDup.TryGetValue("fileDate   ", out objfileDate) ? objfileDate.ToString() : "";
                        object objaddedDate = lastDup.TryGetValue("addedDate  ", out objaddedDate) ? objaddedDate.ToString() : "";


                        sqlWriteCmd.Parameters.Add("@noaa_year      ", SqlDbType.Int, 12).Value = objnoaa_year;
                        sqlWriteCmd.Parameters.Add("@noaa_jday      ", SqlDbType.Int, 12).Value = objnoaa_jday;
                        sqlWriteCmd.Parameters.Add("@noaa_month     ", SqlDbType.Int, 12).Value = objnoaa_month;
                        sqlWriteCmd.Parameters.Add("@noaa_day       ", SqlDbType.Int, 12).Value = objnoaa_day;
                        sqlWriteCmd.Parameters.Add("@noaa_hour      ", SqlDbType.Int, 12).Value = objnoaa_hour;
                        sqlWriteCmd.Parameters.Add("@noaa_min       ", SqlDbType.Int, 12).Value = objnoaa_min;
                        sqlWriteCmd.Parameters.Add("@dt", SqlDbType.Decimal, 16).Value = objdt;
                        sqlWriteCmd.Parameters["@dt"].Precision = 7;
                        sqlWriteCmd.Parameters["@dt"].Scale = 3;
                        sqlWriteCmd.Parameters.Add("@min_dt", SqlDbType.Decimal, 16).Value = objmin_dt;
                        sqlWriteCmd.Parameters["@min_dt"].Precision = 7;
                        sqlWriteCmd.Parameters["@min_dt"].Scale = 3;
                        sqlWriteCmd.Parameters.Add("@max_dt", SqlDbType.Decimal, 16).Value = objmax_dt;
                        sqlWriteCmd.Parameters["@max_dt"].Precision = 7;
                        sqlWriteCmd.Parameters["@max_dt"].Scale = 3;
                        sqlWriteCmd.Parameters.Add("@zen", SqlDbType.Decimal, 16).Value = objzen;
                        sqlWriteCmd.Parameters["@zen"].Precision = 7;
                        sqlWriteCmd.Parameters["@zen"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@min_zen", SqlDbType.Decimal, 16).Value = objmin_zen;
                        sqlWriteCmd.Parameters["@min_zen"].Precision = 7;
                        sqlWriteCmd.Parameters["@min_zen"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@max_zen", SqlDbType.Decimal, 16).Value = objmax_zen;
                        sqlWriteCmd.Parameters["@max_zen"].Precision = 7;
                        sqlWriteCmd.Parameters["@max_zen"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@dw_solar", SqlDbType.Decimal, 16).Value = objdw_solar;
                        sqlWriteCmd.Parameters["@dw_solar"].Precision = 8;
                        sqlWriteCmd.Parameters["@dw_solar"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_dw_solar", SqlDbType.Decimal, 16).Value = objmin_dw_solar;
                        sqlWriteCmd.Parameters["@min_dw_solar"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_dw_solar"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_dw_solar", SqlDbType.Decimal, 16).Value = objmax_dw_solar;
                        sqlWriteCmd.Parameters["@max_dw_solar"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_dw_solar"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@dw_solar_qc    ", SqlDbType.Int, 12).Value = objdw_solar_qc;
                        sqlWriteCmd.Parameters.Add("@uw_solar", SqlDbType.Decimal, 16).Value = objuw_solar;
                        sqlWriteCmd.Parameters["@uw_solar"].Precision = 8;
                        sqlWriteCmd.Parameters["@uw_solar"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_uw_solar", SqlDbType.Decimal, 16).Value = objmin_uw_solar;
                        sqlWriteCmd.Parameters["@min_uw_solar"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_uw_solar"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_uw_solar", SqlDbType.Decimal, 16).Value = objmax_uw_solar;
                        sqlWriteCmd.Parameters["@max_uw_solar"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_uw_solar"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@uw_solar_qc", SqlDbType.Int, 12).Value = objuw_solar_qc;
                        sqlWriteCmd.Parameters.Add("@direct_n", SqlDbType.Decimal, 16).Value = objdirect_n;
                        sqlWriteCmd.Parameters["@direct_n"].Precision = 8;
                        sqlWriteCmd.Parameters["@direct_n"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_direct_n", SqlDbType.Decimal, 16).Value = objmin_direct_n;
                        sqlWriteCmd.Parameters["@min_direct_n"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_direct_n"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_direct_n", SqlDbType.Decimal, 16).Value = objmax_direct_n;
                        sqlWriteCmd.Parameters["@max_direct_n"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_direct_n"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@direct_n_qc", SqlDbType.Int, 12).Value = objdirect_n_qc;
                        sqlWriteCmd.Parameters.Add("@diffuse", SqlDbType.Decimal, 16).Value = objdiffuse;
                        sqlWriteCmd.Parameters["@diffuse"].Precision = 8;
                        sqlWriteCmd.Parameters["@diffuse"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_diffuse", SqlDbType.Decimal, 16).Value = objmin_diffuse;
                        sqlWriteCmd.Parameters["@min_diffuse"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_diffuse"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_diffuse", SqlDbType.Decimal, 16).Value = objmax_diffuse;
                        sqlWriteCmd.Parameters["@max_diffuse"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_diffuse"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@diffuse_qc", SqlDbType.Int, 12).Value = objdiffuse_qc;
                        sqlWriteCmd.Parameters.Add("@dw_ir", SqlDbType.Decimal, 16).Value = objdw_ir;
                        sqlWriteCmd.Parameters["@dw_ir"].Precision = 8;
                        sqlWriteCmd.Parameters["@dw_ir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_dw_ir", SqlDbType.Decimal, 16).Value = objmin_dw_ir;
                        sqlWriteCmd.Parameters["@min_dw_ir"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_dw_ir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_dw_ir", SqlDbType.Decimal, 16).Value = objmax_dw_ir;
                        sqlWriteCmd.Parameters["@max_dw_ir"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_dw_ir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@dw_ir_qc", SqlDbType.Int, 12).Value = objdw_ir_qc;
                        sqlWriteCmd.Parameters.Add("@dw_casetemp", SqlDbType.Decimal, 16).Value = objdw_casetemp;
                        sqlWriteCmd.Parameters["@dw_casetemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@dw_casetemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@min_dw_casetemp", SqlDbType.Decimal, 16).Value = objmin_dw_casetemp;
                        sqlWriteCmd.Parameters["@min_dw_casetemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@min_dw_casetemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@max_dw_casetemp", SqlDbType.Decimal, 16).Value = objmax_dw_casetemp;
                        sqlWriteCmd.Parameters["@max_dw_casetemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@max_dw_casetemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@dw_casetemp_qc", SqlDbType.Int, 12).Value = objdw_casetemp_qc;
                        sqlWriteCmd.Parameters.Add("@dw_dometemp", SqlDbType.Decimal, 16).Value = objdw_dometemp;
                        sqlWriteCmd.Parameters["@dw_dometemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@dw_dometemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@min_dw_dometemp", SqlDbType.Decimal, 16).Value = objmin_dw_dometemp;
                        sqlWriteCmd.Parameters["@min_dw_dometemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@min_dw_dometemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@max_dw_dometemp", SqlDbType.Decimal, 16).Value = objmax_dw_dometemp;
                        sqlWriteCmd.Parameters["@max_dw_dometemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@max_dw_dometemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@dw_dometemp_qc", SqlDbType.Int, 12).Value = objdw_dometemp_qc;
                        sqlWriteCmd.Parameters.Add("@uw_ir", SqlDbType.Decimal, 16).Value = objuw_ir;
                        sqlWriteCmd.Parameters["@uw_ir"].Precision = 8;
                        sqlWriteCmd.Parameters["@uw_ir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_uw_ir", SqlDbType.Decimal, 16).Value = objmin_uw_ir;
                        sqlWriteCmd.Parameters["@min_uw_ir"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_uw_ir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_uw_ir", SqlDbType.Decimal, 16).Value = objmax_uw_ir;
                        sqlWriteCmd.Parameters["@max_uw_ir"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_uw_ir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@uw_ir_qc", SqlDbType.Int, 12).Value = objuw_ir_qc;
                        sqlWriteCmd.Parameters.Add("@uw_casetemp", SqlDbType.Decimal, 16).Value = objuw_casetemp;
                        sqlWriteCmd.Parameters["@uw_casetemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@uw_casetemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@min_uw_casetemp", SqlDbType.Decimal, 16).Value = objmin_uw_casetemp;
                        sqlWriteCmd.Parameters["@min_uw_casetemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@min_uw_casetemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@max_uw_casetemp", SqlDbType.Decimal, 16).Value = objmax_uw_casetemp;
                        sqlWriteCmd.Parameters["@max_uw_casetemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@max_uw_casetemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@uw_casetemp_qc", SqlDbType.Int, 12).Value = objuw_casetemp_qc;
                        sqlWriteCmd.Parameters.Add("@uw_dometemp", SqlDbType.Decimal, 16).Value = objuw_dometemp;
                        sqlWriteCmd.Parameters["@uw_dometemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@uw_dometemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@min_uw_dometemp", SqlDbType.Decimal, 16).Value = objmin_uw_dometemp;
                        sqlWriteCmd.Parameters["@min_uw_dometemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@min_uw_dometemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@max_uw_dometemp", SqlDbType.Decimal, 16).Value = objmax_uw_dometemp;
                        sqlWriteCmd.Parameters["@max_uw_dometemp"].Precision = 9;
                        sqlWriteCmd.Parameters["@max_uw_dometemp"].Scale = 2;
                        sqlWriteCmd.Parameters.Add("@uw_dometemp_qc", SqlDbType.Int, 12).Value = objuw_dometemp_qc;
                        sqlWriteCmd.Parameters.Add("@uvb", SqlDbType.Decimal, 16).Value = objuvb;
                        sqlWriteCmd.Parameters["@uvb"].Precision = 8;
                        sqlWriteCmd.Parameters["@uvb"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_uvb", SqlDbType.Decimal, 16).Value = objmin_uvb;
                        sqlWriteCmd.Parameters["@min_uvb"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_uvb"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_uvb", SqlDbType.Decimal, 16).Value = objmax_uvb;
                        sqlWriteCmd.Parameters["@max_uvb"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_uvb"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@uvb_qc", SqlDbType.Int, 12).Value = objuvb_qc;
                        sqlWriteCmd.Parameters.Add("@par", SqlDbType.Decimal, 16).Value = objpar;
                        sqlWriteCmd.Parameters["@par"].Precision = 8;
                        sqlWriteCmd.Parameters["@par"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_par", SqlDbType.Decimal, 16).Value = objmin_par;
                        sqlWriteCmd.Parameters["@min_par"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_par"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_par", SqlDbType.Decimal, 16).Value = objmax_par;
                        sqlWriteCmd.Parameters["@max_par"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_par"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@par_qc", SqlDbType.Int, 12).Value = objpar_qc;
                        sqlWriteCmd.Parameters.Add("@netsolar", SqlDbType.Decimal, 16).Value = objnetsolar;
                        sqlWriteCmd.Parameters["@netsolar"].Precision = 8;
                        sqlWriteCmd.Parameters["@netsolar"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_netsolar", SqlDbType.Decimal, 16).Value = objmin_netsolar;
                        sqlWriteCmd.Parameters["@min_netsolar"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_netsolar"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_netsolar", SqlDbType.Decimal, 16).Value = objmax_netsolar;
                        sqlWriteCmd.Parameters["@max_netsolar"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_netsolar"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@netsolar_qc", SqlDbType.Int, 12).Value = objnetsolar_qc;
                        sqlWriteCmd.Parameters.Add("@netir", SqlDbType.Decimal, 16).Value = objnetir;
                        sqlWriteCmd.Parameters["@netir"].Precision = 8;
                        sqlWriteCmd.Parameters["@netir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_netir", SqlDbType.Decimal, 16).Value = objmin_netir;
                        sqlWriteCmd.Parameters["@min_netir"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_netir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_netir", SqlDbType.Decimal, 16).Value = objmax_netir;
                        sqlWriteCmd.Parameters["@max_netir"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_netir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@netir_qc", SqlDbType.Int, 12).Value = objnetir_qc;
                        sqlWriteCmd.Parameters.Add("@totalnet", SqlDbType.Decimal, 16).Value = objtotalnet;
                        sqlWriteCmd.Parameters["@totalnet"].Precision = 8;
                        sqlWriteCmd.Parameters["@totalnet"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_totalnet", SqlDbType.Decimal, 16).Value = objmin_totalnet;
                        sqlWriteCmd.Parameters["@min_totalnet"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_totalnet"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_totalnet", SqlDbType.Decimal, 16).Value = objmax_totalnet;
                        sqlWriteCmd.Parameters["@max_totalnet"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_totalnet"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@totalnet_qc", SqlDbType.Int, 12).Value = objtotalnet_qc;
                        sqlWriteCmd.Parameters.Add("@temp", SqlDbType.Decimal, 16).Value = objtemp;
                        sqlWriteCmd.Parameters["@temp"].Precision = 8;
                        sqlWriteCmd.Parameters["@temp"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_temp", SqlDbType.Decimal, 16).Value = objmin_temp;
                        sqlWriteCmd.Parameters["@min_temp"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_temp"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_temp", SqlDbType.Decimal, 16).Value = objmax_temp;
                        sqlWriteCmd.Parameters["@max_temp"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_temp"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@temp_qc", SqlDbType.Int, 12).Value = objtemp_qc;
                        sqlWriteCmd.Parameters.Add("@rh", SqlDbType.Decimal, 16).Value = objrh;
                        sqlWriteCmd.Parameters["@rh"].Precision = 8;
                        sqlWriteCmd.Parameters["@rh"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_rh", SqlDbType.Decimal, 16).Value = objmin_rh;
                        sqlWriteCmd.Parameters["@min_rh"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_rh"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_rh", SqlDbType.Decimal, 16).Value = objmax_rh;
                        sqlWriteCmd.Parameters["@max_rh"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_rh"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@rh_qc", SqlDbType.Int, 12).Value = objrh_qc;
                        sqlWriteCmd.Parameters.Add("@windspd", SqlDbType.Decimal, 16).Value = objwindspd;
                        sqlWriteCmd.Parameters["@windspd"].Precision = 8;
                        sqlWriteCmd.Parameters["@windspd"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_windspd", SqlDbType.Decimal, 16).Value = objmin_windspd;
                        sqlWriteCmd.Parameters["@min_windspd"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_windspd"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_windspd", SqlDbType.Decimal, 16).Value = objmax_windspd;
                        sqlWriteCmd.Parameters["@max_windspd"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_windspd"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@windspd_qc", SqlDbType.Int, 12).Value = objwindspd_qc;
                        sqlWriteCmd.Parameters.Add("@winddir", SqlDbType.Decimal, 16).Value = objwinddir;
                        sqlWriteCmd.Parameters["@winddir"].Precision = 8;
                        sqlWriteCmd.Parameters["@winddir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_winddir", SqlDbType.Decimal, 16).Value = objmin_winddir;
                        sqlWriteCmd.Parameters["@min_winddir"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_winddir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_winddir", SqlDbType.Decimal, 16).Value = objmax_winddir;
                        sqlWriteCmd.Parameters["@max_winddir"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_winddir"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@winddir_qc", SqlDbType.Int, 12).Value = objwinddir_qc;
                        sqlWriteCmd.Parameters.Add("@pressure", SqlDbType.Decimal, 16).Value = objpressure;
                        sqlWriteCmd.Parameters["@pressure"].Precision = 8;
                        sqlWriteCmd.Parameters["@pressure"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@min_pressure", SqlDbType.Decimal, 16).Value = objmin_pressure;
                        sqlWriteCmd.Parameters["@min_pressure"].Precision = 8;
                        sqlWriteCmd.Parameters["@min_pressure"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@max_pressure", SqlDbType.Decimal, 16).Value = objmax_pressure;
                        sqlWriteCmd.Parameters["@max_pressure"].Precision = 8;
                        sqlWriteCmd.Parameters["@max_pressure"].Scale = 1;
                        sqlWriteCmd.Parameters.Add("@pressure_qc", SqlDbType.Int, 12).Value = objpressure_qc;


                        sqlWriteCmd.ExecuteNonQuery();
                        //Console.WriteLine(@"wrote to _dups {0} ", objuniquekeyvalue);

                    }
                    sqlWriteConnection.Close();
                }


            }
            catch (System.Exception e)
            {
                Console.WriteLine("    Caught an exception: " + e.Source + " " + e.Message + " " + e.StackTrace);
                return false;
            }
            return true;
        }








        /// <summary>
        ///  FinalLoader utilizes the SQL Server Always Encrypted libraries in .Net 4.6.1 to load the final data table.
        ///  It tries to model the pattern modeled in the paper by Veronica Peralta
        ///  "Extraction and Integration of MovieLens and IMDb Data".  
        ///  However, SQL Server Always Encrypted prevents us from using server side insert and update queries
        ///  and aggregations, so we model the behavior using loops and dictionary objects in c#.
        ///  
        ///  The weather data files are incremental.  
        ///  Any tables with records inthe incremental files will need to have their original unique key records removed 
        ///  from their respective tables. 
        ///  Records removed from a table will be kept in a "bonepile" table ... in this case: "_out_SurfRad"
        /// </summary>
        /// <returns></returns>
        static bool FinalLoader(DateTime argNoaaIncrementalDate)
        {
            try
            {

                // values needed for aggregator before work begins                   
                DateTime etlStartTime = DateTime.Now;
                DateTime etlEndTime = DateTime.MaxValue;
                int nRecords = 0;
                String tableName = "_in_SurfRad";

                // connection strings 
                SqlConnectionStringBuilder strBuilderRead = new SqlConnectionStringBuilder();
                strBuilderRead.DataSource = "127.0.0.1,31433";
                strBuilderRead.InitialCatalog = "noaaC";
                //strBuilderRead.IntegratedSecurity = true;
                strBuilderRead.UserID = "sa";
                strBuilderRead.Password = "omg.passwords!";
                strBuilderRead.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;

                SqlConnectionStringBuilder strBuilderWrite = new SqlConnectionStringBuilder();
                strBuilderWrite.DataSource = "127.0.0.1,31433";
                strBuilderRead.InitialCatalog = "noaaC";
                //strBuilderRead.IntegratedSecurity = true;
                strBuilderWrite.UserID = "sa";
                strBuilderWrite.Password = "omg.passwords!";
                strBuilderWrite.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;


                // next .... select from _group ... insert to SurfRad
                // Note .... select from _refint ... insert to SurfRad would be the normal approach in the Peralta pattern, but there are no referential integrity enforcements with this master table


                using (SqlConnection sqlReadConnection = new SqlConnection(strBuilderRead.ConnectionString))
                {
                    sqlReadConnection.Open();
                    using (SqlCommand sqlReadCmd = sqlReadConnection.CreateCommand())
                    {
                        sqlReadCmd.CommandText = @"SELECT 
                            noaa_year      , --  int       NULL,      -- year, i.e., 1995                                         Column0     varchar(5)      2018    
                            noaa_jday      , --  int       NULL,      -- Julian day (1 through 365 [or 366])                      Column1     varchar(4)      1  
                            noaa_month     , --  int       NULL,      -- number of the month (1-12)                               Column2     varchar(3)      1  
                            noaa_day       , --  int       NULL,      -- day of the month(1-31)                                   Column3     varchar(3)      1  
                            noaa_hour      , --  int       NULL,      -- hour of the day (0-23)                                   Column4     varchar(3)      0  
                            noaa_min       , --  int       NULL,      -- minute of the hour (0-59)                                Column5     varchar(3)      0  
                            dt             , --  real      NULL,      -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7)      0.000  
                            zen            , --  real      NULL,      -- solar zenith angle (degrees)                             Column7     varchar(7)      93.20     
                            dw_solar       , --  real      NULL,      -- downwelling global solar (Watts m^-2)                    Column8     varchar(8)      0.0 
                            dw_solar_qc    , --  int       NULL,      --                                                          Column9     varchar(2)      0     
                            uw_solar       , --  real      NULL,      -- upwelling global solar (Watts m^-2)                      Column10    varchar(8)      0.4 
                            uw_solar_qc    , --  int       NULL,      --                                                          Column11    varchar(2)      0     
                            direct_n       , --  real      NULL,      -- direct-normal solar (Watts m^-2)                         Column12    varchar(8)      0.8 
                            direct_n_qc    , --  int       NULL,      --                                                          Column13    varchar(2)      0     
                            diffuse        , --  real      NULL,      -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8)      0.7 
                            diffuse_qc     , --  int       NULL,      --                                                          Column15    varchar(2)      0   
                            dw_ir          , --  real      NULL,      -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8)      239.3 
                            dw_ir_qc       , --  int       NULL,      --                                                          Column17    varchar(2)      0   
                            dw_casetemp    , --  real      NULL,      -- downwelling IR case temp. (K)                            Column18    varchar(9)      262.88 
                            dw_casetemp_qc , --  int       NULL,      --                                                          Column19    varchar(2)      0   
                            dw_dometemp    , --  real      NULL,      -- downwelling IR dome temp. (K)                            Column20    varchar(9)      262.79 
                            dw_dometemp_qc , --  int       NULL,      --                                                          Column21    varchar(2)      0   
                            uw_ir          , --  real      NULL,      -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8)      272.9 
                            uw_ir_qc       , --  int       NULL,      --                                                          Column23    varchar(2)      0   
                            uw_casetemp    , --  real      NULL,      -- upwelling IR case temp. (K)                              Column24    varchar(9)      262.34 
                            uw_casetemp_qc , --  int       NULL,      --                                                          Column25    varchar(2)      0   
                            uw_dometemp    , --  real      NULL,      -- upwelling IR dome temp. (K)                              Column26    varchar(9)      262.23 
                            uw_dometemp_qc , --  int       NULL,      --                                                          Column27    varchar(2)      0     
                            uvb            , --  real      NULL,      -- global UVB (milliWatts m^-2)                             Column28    varchar(8)      0.0 
                            uvb_qc         , --  int       NULL,      --                                                          Column29    varchar(2)      0     
                            par            , --  real      NULL,      -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8)      0.4 
                            par_qc         , --  int       NULL,      --                                                          Column31    varchar(2)      0     
                            netsolar       , --  real      NULL,      -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8)      0.4 
                            netsolar_qc    , --  int       NULL,      --                                                          Column33    varchar(2)      0   
                            netir          , --  real      NULL,      -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8)      -33.6 
                            netir_qc       , --  int       NULL,      --                                                          Column35    varchar(2)      0   
                            totalnet       , --  real      NULL,      -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8)      -33.2 
                            totalnet_qc    , --  int       NULL,      --                                                          Column37    varchar(2)      0   
                            temp           , --  real      NULL,      -- 10-meter air temperature (?C)                            Column38    varchar(8)      -10.4 
                            temp_qc        , --  int       NULL,      --                                                          Column39    varchar(2)      0    
                            rh             , --  real      NULL,      -- relative humidity (%)                                    Column40    varchar(8)      92.1 
                            rh_qc          , --  int       NULL,      --                                                          Column41    varchar(2)      0     
                            windspd        , --  real      NULL,      -- wind speed (ms^-1)                                       Column42    varchar(8)      0.8 
                            windspd_qc     , --  int       NULL,      --                                                          Column43    varchar(2)      0   
                            winddir        , --  real      NULL,      -- wind direction (degrees, clockwise from north)           Column44    varchar(8)      110.1 
                            winddir_qc     , --  int       NULL,      --                                                          Column45    varchar(2)      0   
                            pressure       , --  real      NULL,      -- station pressure (mb)                                    Column46    varchar(8)      837.1 
                            pressure_qc    , --  int       NULL       --                                                          Column47    varchar(2)      0
                            file_date      , --  datetime  NULL,      -- noaa filedate (encoded from DayOfYear)    
                            added_date       --  datetime  NULL       -- date data added to database, often delayed
                        FROM _in_group_SurfRad;";      // Note that _refint table would usually be the source for the final load

                        using (SqlDataReader reader = sqlReadCmd.ExecuteReader(CommandBehavior.SequentialAccess))
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    //Console.WriteLine(@"{0} {1}", reader[0], reader[13]);

                                    var noaa_year = (reader[0] as Int32?) ?? 0;                   // int       NULL,     -- year, i.e., 1995                                         Column0     varchar(5) 
                                    var noaa_jday = (reader[1] as Int32?) ?? 0;                   // int       NULL,     -- Julian day (1 through 365 [or 366])                      Column1     varchar(4) 
                                    var noaa_month = (reader[2] as Int32?) ?? 0;                   // int       NULL,     -- number of the month (1-12)                               Column2     varchar(3) 
                                    var noaa_day = (reader[3] as Int32?) ?? 0;                   // int       NULL,     -- day of the month(1-31)                                   Column3     varchar(3) 
                                    var noaa_hour = (reader[4] as Int32?) ?? 0;                   // int       NULL,     -- hour of the day (0-23)                                   Column4     varchar(3) 
                                    var noaa_min = (reader[5] as Int32?) ?? 0;                   // int       NULL,     -- minute of the hour (0-59)                                Column5     varchar(3) 
                                    var dt = (reader[6] as Decimal?) ?? 0;                 // real      NULL,     -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7) 
                                    var zen = (reader[7] as Decimal?) ?? 0;                 // real      NULL,     -- solar zenith angle (degrees)                             Column7     varchar(7) 
                                    var dw_solar = (reader[8] as Decimal?) ?? 0;                 // real      NULL,     -- downwelling global solar (Watts m^-2)                    Column8     varchar(8) 
                                    var dw_solar_qc = (reader[9] as Int32?) ?? 0;                   // int       NULL,     --                                                          Column9     varchar(2) 
                                    var uw_solar = (reader[10] as Decimal?) ?? 0;                // real      NULL,     -- upwelling global solar (Watts m^-2)                      Column10    varchar(8) 
                                    var uw_solar_qc = (reader[11] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column11    varchar(2) 
                                    var direct_n = (reader[12] as Decimal?) ?? 0;                // real      NULL,     -- direct-normal solar (Watts m^-2)                         Column12    varchar(8) 
                                    var direct_n_qc = (reader[13] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column13    varchar(2) 
                                    var diffuse = (reader[14] as Decimal?) ?? 0;                // real      NULL,     -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8) 
                                    var diffuse_qc = (reader[15] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column15    varchar(2) 
                                    var dw_ir = (reader[16] as Decimal?) ?? 0;                // real      NULL,     -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8) 
                                    var dw_ir_qc = (reader[17] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column17    varchar(2) 
                                    var dw_casetemp = (reader[18] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR case temp. (K)                            Column18    varchar(9) 
                                    var dw_casetemp_qc = (reader[19] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column19    varchar(2) 
                                    var dw_dometemp = (reader[20] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR dome temp. (K)                            Column20    varchar(9) 
                                    var dw_dometemp_qc = (reader[21] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column21    varchar(2) 
                                    var uw_ir = (reader[22] as Decimal?) ?? 0;                // real      NULL,     -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8) 
                                    var uw_ir_qc = (reader[23] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column23    varchar(2) 
                                    var uw_casetemp = (reader[24] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR case temp. (K)                              Column24    varchar(9) 
                                    var uw_casetemp_qc = (reader[25] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column25    varchar(2) 
                                    var uw_dometemp = (reader[26] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR dome temp. (K)                              Column26    varchar(9) 
                                    var uw_dometemp_qc = (reader[27] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column27    varchar(2) 
                                    var uvb = (reader[28] as Decimal?) ?? 0;                // real      NULL,     -- global UVB (milliWatts m^-2)                             Column28    varchar(8) 
                                    var uvb_qc = (reader[29] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column29    varchar(2) 
                                    var par = (reader[30] as Decimal?) ?? 0;                // real      NULL,     -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8) 
                                    var par_qc = (reader[31] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column31    varchar(2) 
                                    var netsolar = (reader[32] as Decimal?) ?? 0;                // real      NULL,     -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8) 
                                    var netsolar_qc = (reader[33] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column33    varchar(2) 
                                    var netir = (reader[34] as Decimal?) ?? 0;                // real      NULL,     -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8) 
                                    var netir_qc = (reader[35] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column35    varchar(2) 
                                    var totalnet = (reader[36] as Decimal?) ?? 0;                // real      NULL,     -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8) 
                                    var totalnet_qc = (reader[37] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column37    varchar(2) 
                                    var temp = (reader[38] as Decimal?) ?? 0;                // real      NULL,     -- 10-meter air temperature (?C)                            Column38    varchar(8) 
                                    var temp_qc = (reader[39] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column39    varchar(2) 
                                    var rh = (reader[40] as Decimal?) ?? 0;                // real      NULL,     -- relative humidity (%)                                    Column40    varchar(8) 
                                    var rh_qc = (reader[41] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column41    varchar(2) 
                                    var windspd = (reader[42] as Decimal?) ?? 0;                // real      NULL,     -- wind speed (ms^-1)                                       Column42    varchar(8) 
                                    var windspd_qc = (reader[43] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column43    varchar(2) 
                                    var winddir = (reader[44] as Decimal?) ?? 0;                // real      NULL,     -- wind direction (degrees, clockwise from north)           Column44    varchar(8) 
                                    var winddir_qc = (reader[45] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column45    varchar(2) 
                                    var pressure = (reader[46] as Decimal?) ?? 0;                // real      NULL,     -- station pressure (mb)                                    Column46    varchar(8) 
                                    var pressure_qc = (reader[47] as Int32?) ?? 0;                  // int       NULL      --                                                          Column47    varchar(2) 
                                    var fileDate = (reader[48] as DateTime?) ?? DateTime.MinValue;
                                    var addedDate = (reader[49] as DateTime?) ?? DateTime.MinValue;

                                    using (SqlConnection sqlWriteConnection = new SqlConnection(strBuilderWrite.ConnectionString))
                                    {

                                        sqlWriteConnection.Open();
                                        using (SqlCommand sqlWriteCmd = sqlWriteConnection.CreateCommand())
                                        {
                                            sqlWriteCmd.CommandText = @"INSERT INTO _in_SurfRad (
                                                noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min, dt, zen, dw_solar, dw_solar_qc, uw_solar, uw_solar_qc, direct_n, direct_n_qc, diffuse, diffuse_qc, dw_ir, dw_ir_qc, dw_casetemp, dw_casetemp_qc, dw_dometemp, dw_dometemp_qc, uw_ir, uw_ir_qc, uw_casetemp, uw_casetemp_qc, uw_dometemp, uw_dometemp_qc, uvb, uvb_qc, par, par_qc, netsolar, netsolar_qc, netir, netir_qc, totalnet, totalnet_qc, temp, temp_qc, rh, rh_qc, windspd, windspd_qc, winddir, winddir_qc, pressure, pressure_qc, fileDate, addedDate
                                                ) 
                                                VALUES 
                                                (
                                                @noaa_year, @noaa_jday, @noaa_month, @noaa_day, @noaa_hour, @noaa_min, @dt, @zen, @dw_solar, @dw_solar_qc, @uw_solar, @uw_solar_qc, @direct_n, @direct_n_qc, @diffuse, @diffuse_qc, @dw_ir, @dw_ir_qc, @dw_casetemp, @dw_casetemp_qc, @dw_dometemp, @dw_dometemp_qc, @uw_ir, @uw_ir_qc, @uw_casetemp, @uw_casetemp_qc, @uw_dometemp, @uw_dometemp_qc, @uvb, @uvb_qc, @par, @par_qc, @netsolar, @netsolar_qc, @netir, @netir_qc, @totalnet, @totalnet_qc, @temp, @temp_qc, @rh, @rh_qc, @windspd, @windspd_qc, @winddir, @winddir_qc, @pressure, @pressure_qc, @fileDate, @addedDate
                                                )";


                                            sqlWriteCmd.Parameters.Add("@noaa_year      ", SqlDbType.Int, 12).Value = noaa_year;
                                            sqlWriteCmd.Parameters.Add("@noaa_jday      ", SqlDbType.Int, 12).Value = noaa_jday;
                                            sqlWriteCmd.Parameters.Add("@noaa_month     ", SqlDbType.Int, 12).Value = noaa_month;
                                            sqlWriteCmd.Parameters.Add("@noaa_day       ", SqlDbType.Int, 12).Value = noaa_day;
                                            sqlWriteCmd.Parameters.Add("@noaa_hour      ", SqlDbType.Int, 12).Value = noaa_hour;
                                            sqlWriteCmd.Parameters.Add("@noaa_min       ", SqlDbType.Int, 12).Value = noaa_min;
                                            sqlWriteCmd.Parameters.Add("@dt", SqlDbType.Decimal, 16).Value = dt;
                                            sqlWriteCmd.Parameters["@dt"].Precision = 7;
                                            sqlWriteCmd.Parameters["@dt"].Scale = 3;
                                            sqlWriteCmd.Parameters.Add("@zen", SqlDbType.Decimal, 16).Value = zen;
                                            sqlWriteCmd.Parameters["@zen"].Precision = 7;
                                            sqlWriteCmd.Parameters["@zen"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_solar", SqlDbType.Decimal, 16).Value = dw_solar;
                                            sqlWriteCmd.Parameters["@dw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_solar_qc    ", SqlDbType.Int, 12).Value = dw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_solar", SqlDbType.Decimal, 16).Value = uw_solar;
                                            sqlWriteCmd.Parameters["@uw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_solar_qc", SqlDbType.Int, 12).Value = uw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@direct_n", SqlDbType.Decimal, 16).Value = direct_n;
                                            sqlWriteCmd.Parameters["@direct_n"].Precision = 8;
                                            sqlWriteCmd.Parameters["@direct_n"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@direct_n_qc", SqlDbType.Int, 12).Value = direct_n_qc;
                                            sqlWriteCmd.Parameters.Add("@diffuse", SqlDbType.Decimal, 16).Value = diffuse;
                                            sqlWriteCmd.Parameters["@diffuse"].Precision = 8;
                                            sqlWriteCmd.Parameters["@diffuse"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@diffuse_qc", SqlDbType.Int, 12).Value = diffuse_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_ir", SqlDbType.Decimal, 16).Value = dw_ir;
                                            sqlWriteCmd.Parameters["@dw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_ir_qc", SqlDbType.Int, 12).Value = dw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp", SqlDbType.Decimal, 16).Value = dw_casetemp;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp_qc", SqlDbType.Int, 12).Value = dw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp", SqlDbType.Decimal, 16).Value = dw_dometemp;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp_qc", SqlDbType.Int, 12).Value = dw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_ir", SqlDbType.Decimal, 16).Value = uw_ir;
                                            sqlWriteCmd.Parameters["@uw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_ir_qc", SqlDbType.Int, 12).Value = uw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp", SqlDbType.Decimal, 16).Value = uw_casetemp;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp_qc", SqlDbType.Int, 12).Value = uw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp", SqlDbType.Decimal, 16).Value = uw_dometemp;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp_qc", SqlDbType.Int, 12).Value = uw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uvb", SqlDbType.Decimal, 16).Value = uvb;
                                            sqlWriteCmd.Parameters["@uvb"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uvb"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uvb_qc", SqlDbType.Int, 12).Value = uvb_qc;
                                            sqlWriteCmd.Parameters.Add("@par", SqlDbType.Decimal, 16).Value = par;
                                            sqlWriteCmd.Parameters["@par"].Precision = 8;
                                            sqlWriteCmd.Parameters["@par"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@par_qc", SqlDbType.Int, 12).Value = par_qc;
                                            sqlWriteCmd.Parameters.Add("@netsolar", SqlDbType.Decimal, 16).Value = netsolar;
                                            sqlWriteCmd.Parameters["@netsolar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netsolar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netsolar_qc", SqlDbType.Int, 12).Value = netsolar_qc;
                                            sqlWriteCmd.Parameters.Add("@netir", SqlDbType.Decimal, 16).Value = netir;
                                            sqlWriteCmd.Parameters["@netir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netir_qc", SqlDbType.Int, 12).Value = netir_qc;
                                            sqlWriteCmd.Parameters.Add("@totalnet", SqlDbType.Decimal, 16).Value = totalnet;
                                            sqlWriteCmd.Parameters["@totalnet"].Precision = 8;
                                            sqlWriteCmd.Parameters["@totalnet"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@totalnet_qc", SqlDbType.Int, 12).Value = totalnet_qc;
                                            sqlWriteCmd.Parameters.Add("@temp", SqlDbType.Decimal, 16).Value = temp;
                                            sqlWriteCmd.Parameters["@temp"].Precision = 8;
                                            sqlWriteCmd.Parameters["@temp"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@temp_qc", SqlDbType.Int, 12).Value = temp_qc;
                                            sqlWriteCmd.Parameters.Add("@rh", SqlDbType.Decimal, 16).Value = rh;
                                            sqlWriteCmd.Parameters["@rh"].Precision = 8;
                                            sqlWriteCmd.Parameters["@rh"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@rh_qc", SqlDbType.Int, 12).Value = rh_qc;
                                            sqlWriteCmd.Parameters.Add("@windspd", SqlDbType.Decimal, 16).Value = windspd;
                                            sqlWriteCmd.Parameters["@windspd"].Precision = 8;
                                            sqlWriteCmd.Parameters["@windspd"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@windspd_qc", SqlDbType.Int, 12).Value = windspd_qc;
                                            sqlWriteCmd.Parameters.Add("@winddir", SqlDbType.Decimal, 16).Value = winddir;
                                            sqlWriteCmd.Parameters["@winddir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@winddir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@winddir_qc", SqlDbType.Int, 12).Value = winddir_qc;
                                            sqlWriteCmd.Parameters.Add("@pressure", SqlDbType.Decimal, 16).Value = pressure;
                                            sqlWriteCmd.Parameters["@pressure"].Precision = 8;
                                            sqlWriteCmd.Parameters["@pressure"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@pressure_qc", SqlDbType.Int, 12).Value = pressure_qc;

                                            //Console.WriteLine(@"ready to write to _clean {0} {1} {2} {3}", accountKey, DOB, age, ageAsOfDay);
                                            //sqlWriteCmd.CommandTimeout = 0; // for debugging only ... turns out the problem lied with nulls and nvarchar vs varchar
                                            sqlWriteCmd.ExecuteNonQuery();
                                            //Console.WriteLine(@"wrote to _clean {0} {1} {2} {3}", accountKey, DOB, age, ageAsOfDay);

                                        }
                                        sqlWriteConnection.Close();
                                    }
                                }
                            }
                        }
                    }
                    sqlReadConnection.Close();

                    // values needed for aggregator after work complete                   
                    etlEndTime = DateTime.Now;
                    nRecords = Counter(tableName);
                    if (!Aggregator(etlStartTime, etlEndTime, tableName, nRecords, argNoaaIncrementalDate))
                    {
                        throw new System.Exception("error encountered during Aggregator");
                    }

                }


                //
                // we're not done with the finalloader for incremental tables ... next move rows to be deleted to the bonepile ... in this case: "_out_SurfRad"
                //


                // values needed for aggregator before work begins                   
                etlStartTime = DateTime.Now;
                nRecords = 0;
                tableName = "_out_SurfRad";

                // next .... select from bulk table SurfRad ... insert to _out_SurfRad
                // Note .... select from _refint ... insert to SurfRad would be the normal approach in the Peralta pattern, but there are no referential integrity enforcements with this master table

                using (SqlConnection sqlReadConnection = new SqlConnection(strBuilderRead.ConnectionString))
                {
                    sqlReadConnection.Open();
                    using (SqlCommand sqlReadCmd = sqlReadConnection.CreateCommand())
                    {
                        sqlReadCmd.CommandText = @"SELECT 
                            noaa_year      , --  int       NULL,      -- year, i.e., 1995                                         Column0     varchar(5)      2018    
                            noaa_jday      , --  int       NULL,      -- Julian day (1 through 365 [or 366])                      Column1     varchar(4)      1  
                            noaa_month     , --  int       NULL,      -- number of the month (1-12)                               Column2     varchar(3)      1  
                            noaa_day       , --  int       NULL,      -- day of the month(1-31)                                   Column3     varchar(3)      1  
                            noaa_hour      , --  int       NULL,      -- hour of the day (0-23)                                   Column4     varchar(3)      0  
                            noaa_min       , --  int       NULL,      -- minute of the hour (0-59)                                Column5     varchar(3)      0  
                            dt             , --  real      NULL,      -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7)      0.000  
                            zen            , --  real      NULL,      -- solar zenith angle (degrees)                             Column7     varchar(7)      93.20     
                            dw_solar       , --  real      NULL,      -- downwelling global solar (Watts m^-2)                    Column8     varchar(8)      0.0 
                            dw_solar_qc    , --  int       NULL,      --                                                          Column9     varchar(2)      0     
                            uw_solar       , --  real      NULL,      -- upwelling global solar (Watts m^-2)                      Column10    varchar(8)      0.4 
                            uw_solar_qc    , --  int       NULL,      --                                                          Column11    varchar(2)      0     
                            direct_n       , --  real      NULL,      -- direct-normal solar (Watts m^-2)                         Column12    varchar(8)      0.8 
                            direct_n_qc    , --  int       NULL,      --                                                          Column13    varchar(2)      0     
                            diffuse        , --  real      NULL,      -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8)      0.7 
                            diffuse_qc     , --  int       NULL,      --                                                          Column15    varchar(2)      0   
                            dw_ir          , --  real      NULL,      -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8)      239.3 
                            dw_ir_qc       , --  int       NULL,      --                                                          Column17    varchar(2)      0   
                            dw_casetemp    , --  real      NULL,      -- downwelling IR case temp. (K)                            Column18    varchar(9)      262.88 
                            dw_casetemp_qc , --  int       NULL,      --                                                          Column19    varchar(2)      0   
                            dw_dometemp    , --  real      NULL,      -- downwelling IR dome temp. (K)                            Column20    varchar(9)      262.79 
                            dw_dometemp_qc , --  int       NULL,      --                                                          Column21    varchar(2)      0   
                            uw_ir          , --  real      NULL,      -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8)      272.9 
                            uw_ir_qc       , --  int       NULL,      --                                                          Column23    varchar(2)      0   
                            uw_casetemp    , --  real      NULL,      -- upwelling IR case temp. (K)                              Column24    varchar(9)      262.34 
                            uw_casetemp_qc , --  int       NULL,      --                                                          Column25    varchar(2)      0   
                            uw_dometemp    , --  real      NULL,      -- upwelling IR dome temp. (K)                              Column26    varchar(9)      262.23 
                            uw_dometemp_qc , --  int       NULL,      --                                                          Column27    varchar(2)      0     
                            uvb            , --  real      NULL,      -- global UVB (milliWatts m^-2)                             Column28    varchar(8)      0.0 
                            uvb_qc         , --  int       NULL,      --                                                          Column29    varchar(2)      0     
                            par            , --  real      NULL,      -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8)      0.4 
                            par_qc         , --  int       NULL,      --                                                          Column31    varchar(2)      0     
                            netsolar       , --  real      NULL,      -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8)      0.4 
                            netsolar_qc    , --  int       NULL,      --                                                          Column33    varchar(2)      0   
                            netir          , --  real      NULL,      -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8)      -33.6 
                            netir_qc       , --  int       NULL,      --                                                          Column35    varchar(2)      0   
                            totalnet       , --  real      NULL,      -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8)      -33.2 
                            totalnet_qc    , --  int       NULL,      --                                                          Column37    varchar(2)      0   
                            temp           , --  real      NULL,      -- 10-meter air temperature (?C)                            Column38    varchar(8)      -10.4 
                            temp_qc        , --  int       NULL,      --                                                          Column39    varchar(2)      0    
                            rh             , --  real      NULL,      -- relative humidity (%)                                    Column40    varchar(8)      92.1 
                            rh_qc          , --  int       NULL,      --                                                          Column41    varchar(2)      0     
                            windspd        , --  real      NULL,      -- wind speed (ms^-1)                                       Column42    varchar(8)      0.8 
                            windspd_qc     , --  int       NULL,      --                                                          Column43    varchar(2)      0   
                            winddir        , --  real      NULL,      -- wind direction (degrees, clockwise from north)           Column44    varchar(8)      110.1 
                            winddir_qc     , --  int       NULL,      --                                                          Column45    varchar(2)      0   
                            pressure       , --  real      NULL,      -- station pressure (mb)                                    Column46    varchar(8)      837.1 
                            pressure_qc    , --  int       NULL       --                                                          Column47    varchar(2)      0
                            file_date      , --  datetime  NULL,      -- noaa filedate (encoded from DayOfYear)    
                            added_date       --  datetime  NULL       -- date data added to database, often delayed
                        FROM SurfRad
                        WHERE 
                            UniqueVehicleKey IN (SELECT UniqueVehicleKey FROM _in_NMIVehicle);";


                        using (SqlDataReader reader = sqlReadCmd.ExecuteReader(CommandBehavior.SequentialAccess))
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    //Console.WriteLine(@"{0} {1}", reader[0], reader[13]);

                                    var noaa_year = (reader[0] as Int32?) ?? 0;                   // int       NULL,     -- year, i.e., 1995                                         Column0     varchar(5) 
                                    var noaa_jday = (reader[1] as Int32?) ?? 0;                   // int       NULL,     -- Julian day (1 through 365 [or 366])                      Column1     varchar(4) 
                                    var noaa_month = (reader[2] as Int32?) ?? 0;                   // int       NULL,     -- number of the month (1-12)                               Column2     varchar(3) 
                                    var noaa_day = (reader[3] as Int32?) ?? 0;                   // int       NULL,     -- day of the month(1-31)                                   Column3     varchar(3) 
                                    var noaa_hour = (reader[4] as Int32?) ?? 0;                   // int       NULL,     -- hour of the day (0-23)                                   Column4     varchar(3) 
                                    var noaa_min = (reader[5] as Int32?) ?? 0;                   // int       NULL,     -- minute of the hour (0-59)                                Column5     varchar(3) 
                                    var dt = (reader[6] as Decimal?) ?? 0;                 // real      NULL,     -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7) 
                                    var zen = (reader[7] as Decimal?) ?? 0;                 // real      NULL,     -- solar zenith angle (degrees)                             Column7     varchar(7) 
                                    var dw_solar = (reader[8] as Decimal?) ?? 0;                 // real      NULL,     -- downwelling global solar (Watts m^-2)                    Column8     varchar(8) 
                                    var dw_solar_qc = (reader[9] as Int32?) ?? 0;                   // int       NULL,     --                                                          Column9     varchar(2) 
                                    var uw_solar = (reader[10] as Decimal?) ?? 0;                // real      NULL,     -- upwelling global solar (Watts m^-2)                      Column10    varchar(8) 
                                    var uw_solar_qc = (reader[11] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column11    varchar(2) 
                                    var direct_n = (reader[12] as Decimal?) ?? 0;                // real      NULL,     -- direct-normal solar (Watts m^-2)                         Column12    varchar(8) 
                                    var direct_n_qc = (reader[13] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column13    varchar(2) 
                                    var diffuse = (reader[14] as Decimal?) ?? 0;                // real      NULL,     -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8) 
                                    var diffuse_qc = (reader[15] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column15    varchar(2) 
                                    var dw_ir = (reader[16] as Decimal?) ?? 0;                // real      NULL,     -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8) 
                                    var dw_ir_qc = (reader[17] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column17    varchar(2) 
                                    var dw_casetemp = (reader[18] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR case temp. (K)                            Column18    varchar(9) 
                                    var dw_casetemp_qc = (reader[19] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column19    varchar(2) 
                                    var dw_dometemp = (reader[20] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR dome temp. (K)                            Column20    varchar(9) 
                                    var dw_dometemp_qc = (reader[21] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column21    varchar(2) 
                                    var uw_ir = (reader[22] as Decimal?) ?? 0;                // real      NULL,     -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8) 
                                    var uw_ir_qc = (reader[23] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column23    varchar(2) 
                                    var uw_casetemp = (reader[24] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR case temp. (K)                              Column24    varchar(9) 
                                    var uw_casetemp_qc = (reader[25] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column25    varchar(2) 
                                    var uw_dometemp = (reader[26] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR dome temp. (K)                              Column26    varchar(9) 
                                    var uw_dometemp_qc = (reader[27] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column27    varchar(2) 
                                    var uvb = (reader[28] as Decimal?) ?? 0;                // real      NULL,     -- global UVB (milliWatts m^-2)                             Column28    varchar(8) 
                                    var uvb_qc = (reader[29] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column29    varchar(2) 
                                    var par = (reader[30] as Decimal?) ?? 0;                // real      NULL,     -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8) 
                                    var par_qc = (reader[31] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column31    varchar(2) 
                                    var netsolar = (reader[32] as Decimal?) ?? 0;                // real      NULL,     -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8) 
                                    var netsolar_qc = (reader[33] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column33    varchar(2) 
                                    var netir = (reader[34] as Decimal?) ?? 0;                // real      NULL,     -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8) 
                                    var netir_qc = (reader[35] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column35    varchar(2) 
                                    var totalnet = (reader[36] as Decimal?) ?? 0;                // real      NULL,     -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8) 
                                    var totalnet_qc = (reader[37] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column37    varchar(2) 
                                    var temp = (reader[38] as Decimal?) ?? 0;                // real      NULL,     -- 10-meter air temperature (?C)                            Column38    varchar(8) 
                                    var temp_qc = (reader[39] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column39    varchar(2) 
                                    var rh = (reader[40] as Decimal?) ?? 0;                // real      NULL,     -- relative humidity (%)                                    Column40    varchar(8) 
                                    var rh_qc = (reader[41] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column41    varchar(2) 
                                    var windspd = (reader[42] as Decimal?) ?? 0;                // real      NULL,     -- wind speed (ms^-1)                                       Column42    varchar(8) 
                                    var windspd_qc = (reader[43] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column43    varchar(2) 
                                    var winddir = (reader[44] as Decimal?) ?? 0;                // real      NULL,     -- wind direction (degrees, clockwise from north)           Column44    varchar(8) 
                                    var winddir_qc = (reader[45] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column45    varchar(2) 
                                    var pressure = (reader[46] as Decimal?) ?? 0;                // real      NULL,     -- station pressure (mb)                                    Column46    varchar(8) 
                                    var pressure_qc = (reader[47] as Int32?) ?? 0;                  // int       NULL      --                                                          Column47    varchar(2) 
                                    var fileDate = (reader[48] as DateTime?) ?? DateTime.MinValue;
                                    var addedDate = (reader[49] as DateTime?) ?? DateTime.MinValue;
                                    // keep track of when we're puting this in the bonepile
                                    var removedDate = DateTime.Now;


                                    using (SqlConnection sqlWriteConnection = new SqlConnection(strBuilderWrite.ConnectionString))
                                    {

                                        sqlWriteConnection.Open();
                                        using (SqlCommand sqlWriteCmd = sqlWriteConnection.CreateCommand())
                                        {
                                            sqlWriteCmd.CommandText = @"INSERT INTO _out_SurfRad (
                                                noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min, dt, zen, dw_solar, dw_solar_qc, uw_solar, uw_solar_qc, direct_n, direct_n_qc, diffuse, diffuse_qc, dw_ir, dw_ir_qc, dw_casetemp, dw_casetemp_qc, dw_dometemp, dw_dometemp_qc, uw_ir, uw_ir_qc, uw_casetemp, uw_casetemp_qc, uw_dometemp, uw_dometemp_qc, uvb, uvb_qc, par, par_qc, netsolar, netsolar_qc, netir, netir_qc, totalnet, totalnet_qc, temp, temp_qc, rh, rh_qc, windspd, windspd_qc, winddir, winddir_qc, pressure, pressure_qc, fileDate, addedDate, removedDate
                                                ) 
                                                VALUES 
                                                (
                                                @noaa_year, @noaa_jday, @noaa_month, @noaa_day, @noaa_hour, @noaa_min, @dt, @zen, @dw_solar, @dw_solar_qc, @uw_solar, @uw_solar_qc, @direct_n, @direct_n_qc, @diffuse, @diffuse_qc, @dw_ir, @dw_ir_qc, @dw_casetemp, @dw_casetemp_qc, @dw_dometemp, @dw_dometemp_qc, @uw_ir, @uw_ir_qc, @uw_casetemp, @uw_casetemp_qc, @uw_dometemp, @uw_dometemp_qc, @uvb, @uvb_qc, @par, @par_qc, @netsolar, @netsolar_qc, @netir, @netir_qc, @totalnet, @totalnet_qc, @temp, @temp_qc, @rh, @rh_qc, @windspd, @windspd_qc, @winddir, @winddir_qc, @pressure, @pressure_qc, @fileDate, @addedDate, @removedDate
                                                )";


                                            sqlWriteCmd.Parameters.Add("@noaa_year      ", SqlDbType.Int, 12).Value = noaa_year;
                                            sqlWriteCmd.Parameters.Add("@noaa_jday      ", SqlDbType.Int, 12).Value = noaa_jday;
                                            sqlWriteCmd.Parameters.Add("@noaa_month     ", SqlDbType.Int, 12).Value = noaa_month;
                                            sqlWriteCmd.Parameters.Add("@noaa_day       ", SqlDbType.Int, 12).Value = noaa_day;
                                            sqlWriteCmd.Parameters.Add("@noaa_hour      ", SqlDbType.Int, 12).Value = noaa_hour;
                                            sqlWriteCmd.Parameters.Add("@noaa_min       ", SqlDbType.Int, 12).Value = noaa_min;
                                            sqlWriteCmd.Parameters.Add("@dt", SqlDbType.Decimal, 16).Value = dt;
                                            sqlWriteCmd.Parameters["@dt"].Precision = 7;
                                            sqlWriteCmd.Parameters["@dt"].Scale = 3;
                                            sqlWriteCmd.Parameters.Add("@zen", SqlDbType.Decimal, 16).Value = zen;
                                            sqlWriteCmd.Parameters["@zen"].Precision = 7;
                                            sqlWriteCmd.Parameters["@zen"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_solar", SqlDbType.Decimal, 16).Value = dw_solar;
                                            sqlWriteCmd.Parameters["@dw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_solar_qc    ", SqlDbType.Int, 12).Value = dw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_solar", SqlDbType.Decimal, 16).Value = uw_solar;
                                            sqlWriteCmd.Parameters["@uw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_solar_qc", SqlDbType.Int, 12).Value = uw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@direct_n", SqlDbType.Decimal, 16).Value = direct_n;
                                            sqlWriteCmd.Parameters["@direct_n"].Precision = 8;
                                            sqlWriteCmd.Parameters["@direct_n"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@direct_n_qc", SqlDbType.Int, 12).Value = direct_n_qc;
                                            sqlWriteCmd.Parameters.Add("@diffuse", SqlDbType.Decimal, 16).Value = diffuse;
                                            sqlWriteCmd.Parameters["@diffuse"].Precision = 8;
                                            sqlWriteCmd.Parameters["@diffuse"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@diffuse_qc", SqlDbType.Int, 12).Value = diffuse_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_ir", SqlDbType.Decimal, 16).Value = dw_ir;
                                            sqlWriteCmd.Parameters["@dw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_ir_qc", SqlDbType.Int, 12).Value = dw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp", SqlDbType.Decimal, 16).Value = dw_casetemp;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp_qc", SqlDbType.Int, 12).Value = dw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp", SqlDbType.Decimal, 16).Value = dw_dometemp;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp_qc", SqlDbType.Int, 12).Value = dw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_ir", SqlDbType.Decimal, 16).Value = uw_ir;
                                            sqlWriteCmd.Parameters["@uw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_ir_qc", SqlDbType.Int, 12).Value = uw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp", SqlDbType.Decimal, 16).Value = uw_casetemp;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp_qc", SqlDbType.Int, 12).Value = uw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp", SqlDbType.Decimal, 16).Value = uw_dometemp;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp_qc", SqlDbType.Int, 12).Value = uw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uvb", SqlDbType.Decimal, 16).Value = uvb;
                                            sqlWriteCmd.Parameters["@uvb"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uvb"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uvb_qc", SqlDbType.Int, 12).Value = uvb_qc;
                                            sqlWriteCmd.Parameters.Add("@par", SqlDbType.Decimal, 16).Value = par;
                                            sqlWriteCmd.Parameters["@par"].Precision = 8;
                                            sqlWriteCmd.Parameters["@par"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@par_qc", SqlDbType.Int, 12).Value = par_qc;
                                            sqlWriteCmd.Parameters.Add("@netsolar", SqlDbType.Decimal, 16).Value = netsolar;
                                            sqlWriteCmd.Parameters["@netsolar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netsolar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netsolar_qc", SqlDbType.Int, 12).Value = netsolar_qc;
                                            sqlWriteCmd.Parameters.Add("@netir", SqlDbType.Decimal, 16).Value = netir;
                                            sqlWriteCmd.Parameters["@netir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netir_qc", SqlDbType.Int, 12).Value = netir_qc;
                                            sqlWriteCmd.Parameters.Add("@totalnet", SqlDbType.Decimal, 16).Value = totalnet;
                                            sqlWriteCmd.Parameters["@totalnet"].Precision = 8;
                                            sqlWriteCmd.Parameters["@totalnet"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@totalnet_qc", SqlDbType.Int, 12).Value = totalnet_qc;
                                            sqlWriteCmd.Parameters.Add("@temp", SqlDbType.Decimal, 16).Value = temp;
                                            sqlWriteCmd.Parameters["@temp"].Precision = 8;
                                            sqlWriteCmd.Parameters["@temp"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@temp_qc", SqlDbType.Int, 12).Value = temp_qc;
                                            sqlWriteCmd.Parameters.Add("@rh", SqlDbType.Decimal, 16).Value = rh;
                                            sqlWriteCmd.Parameters["@rh"].Precision = 8;
                                            sqlWriteCmd.Parameters["@rh"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@rh_qc", SqlDbType.Int, 12).Value = rh_qc;
                                            sqlWriteCmd.Parameters.Add("@windspd", SqlDbType.Decimal, 16).Value = windspd;
                                            sqlWriteCmd.Parameters["@windspd"].Precision = 8;
                                            sqlWriteCmd.Parameters["@windspd"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@windspd_qc", SqlDbType.Int, 12).Value = windspd_qc;
                                            sqlWriteCmd.Parameters.Add("@winddir", SqlDbType.Decimal, 16).Value = winddir;
                                            sqlWriteCmd.Parameters["@winddir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@winddir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@winddir_qc", SqlDbType.Int, 12).Value = winddir_qc;
                                            sqlWriteCmd.Parameters.Add("@pressure", SqlDbType.Decimal, 16).Value = pressure;
                                            sqlWriteCmd.Parameters["@pressure"].Precision = 8;
                                            sqlWriteCmd.Parameters["@pressure"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@pressure_qc", SqlDbType.Int, 12).Value = pressure_qc;

                                            // todo:  removedDate

                                            //Console.WriteLine(@"ready to write to _clean {0} {1} {2} {3}", accountKey, DOB, age, ageAsOfDay);
                                            //sqlWriteCmd.CommandTimeout = 0; // for debugging only ... turns out the problem lied with nulls and nvarchar vs varchar
                                            sqlWriteCmd.ExecuteNonQuery();
                                            //Console.WriteLine(@"wrote to _clean {0} {1} {2} {3}", accountKey, DOB, age, ageAsOfDay);

                                        }
                                        sqlWriteConnection.Close();
                                    }
                                }
                            }
                        }
                    }
                    sqlReadConnection.Close();

                    // values needed for aggregator after work complete                   
                    etlEndTime = DateTime.Now;
                    nRecords = Counter(tableName);
                    if (!Aggregator(etlStartTime, etlEndTime, tableName, nRecords, argNoaaIncrementalDate))
                    {
                        throw new System.Exception("error encountered during Aggregator");
                    }

                }


                //
                // we're still not done with the finalloader for incremental tables ... next remove rows to be deleted from the bulk datatable ... in this case: "SurfRad"
                //

                // values needed for aggregator before work begins                   
                etlStartTime = DateTime.Now;
                nRecords = 0;
                tableName = "SurfRad";

                // connection strings 
                SqlConnectionStringBuilder strBuilderPrepare = new SqlConnectionStringBuilder();
                strBuilderPrepare.DataSource = "127.0.0.1,31433";
                strBuilderPrepare.InitialCatalog = "noaaC";
                //strBuilderPrepare.IntegratedSecurity = true;
                strBuilderPrepare.UserID = "sa";
                strBuilderPrepare.Password = "omg.passwords!";
                strBuilderPrepare.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;

                using (SqlConnection sqlPrepConnection = new SqlConnection(strBuilderPrepare.ConnectionString))
                {
                    sqlPrepConnection.Open();
                    using (SqlCommand cmd = sqlPrepConnection.CreateCommand())
                    {
                        cmd.CommandText = @"DELETE 
                        FROM SurfRad
                        WHERE 
                            (noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min) IN (SELECT noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min  FROM _in_SurfRad);";
                        cmd.ExecuteNonQuery();
                        Console.WriteLine(@"removed any duplicate incremental records from SurfRad");
                    }


                    sqlPrepConnection.Close();
                    sqlPrepConnection.Dispose();
                }

                // values needed for aggregator after work complete                   
                etlEndTime = DateTime.Now;
                nRecords = Counter(tableName);
                if (!Aggregator(etlStartTime, etlEndTime, tableName, nRecords, argNoaaIncrementalDate))
                {
                    throw new System.Exception("error encountered during Aggregator");
                }


                //
                // we're still not done with the finalloader for incremental tables ... next add new incremental rows to the bulk datatable ... in this case: from "_in_SurfRad" to "SurfRad"
                //

                // values needed for aggregator before work begins                   
                etlStartTime = DateTime.Now;
                nRecords = 0;
                tableName = "SurfRad";

                // next .... select from _in_SurfRad ... insert to SurfRad

                using (SqlConnection sqlReadConnection = new SqlConnection(strBuilderRead.ConnectionString))
                {
                    sqlReadConnection.Open();
                    using (SqlCommand sqlReadCmd = sqlReadConnection.CreateCommand())
                    {
                        sqlReadCmd.CommandText = @"SELECT 
                            noaa_year      , --  int       NULL,      -- year, i.e., 1995                                         Column0     varchar(5)      2018    
                            noaa_jday      , --  int       NULL,      -- Julian day (1 through 365 [or 366])                      Column1     varchar(4)      1  
                            noaa_month     , --  int       NULL,      -- number of the month (1-12)                               Column2     varchar(3)      1  
                            noaa_day       , --  int       NULL,      -- day of the month(1-31)                                   Column3     varchar(3)      1  
                            noaa_hour      , --  int       NULL,      -- hour of the day (0-23)                                   Column4     varchar(3)      0  
                            noaa_min       , --  int       NULL,      -- minute of the hour (0-59)                                Column5     varchar(3)      0  
                            dt             , --  real      NULL,      -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7)      0.000  
                            zen            , --  real      NULL,      -- solar zenith angle (degrees)                             Column7     varchar(7)      93.20     
                            dw_solar       , --  real      NULL,      -- downwelling global solar (Watts m^-2)                    Column8     varchar(8)      0.0 
                            dw_solar_qc    , --  int       NULL,      --                                                          Column9     varchar(2)      0     
                            uw_solar       , --  real      NULL,      -- upwelling global solar (Watts m^-2)                      Column10    varchar(8)      0.4 
                            uw_solar_qc    , --  int       NULL,      --                                                          Column11    varchar(2)      0     
                            direct_n       , --  real      NULL,      -- direct-normal solar (Watts m^-2)                         Column12    varchar(8)      0.8 
                            direct_n_qc    , --  int       NULL,      --                                                          Column13    varchar(2)      0     
                            diffuse        , --  real      NULL,      -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8)      0.7 
                            diffuse_qc     , --  int       NULL,      --                                                          Column15    varchar(2)      0   
                            dw_ir          , --  real      NULL,      -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8)      239.3 
                            dw_ir_qc       , --  int       NULL,      --                                                          Column17    varchar(2)      0   
                            dw_casetemp    , --  real      NULL,      -- downwelling IR case temp. (K)                            Column18    varchar(9)      262.88 
                            dw_casetemp_qc , --  int       NULL,      --                                                          Column19    varchar(2)      0   
                            dw_dometemp    , --  real      NULL,      -- downwelling IR dome temp. (K)                            Column20    varchar(9)      262.79 
                            dw_dometemp_qc , --  int       NULL,      --                                                          Column21    varchar(2)      0   
                            uw_ir          , --  real      NULL,      -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8)      272.9 
                            uw_ir_qc       , --  int       NULL,      --                                                          Column23    varchar(2)      0   
                            uw_casetemp    , --  real      NULL,      -- upwelling IR case temp. (K)                              Column24    varchar(9)      262.34 
                            uw_casetemp_qc , --  int       NULL,      --                                                          Column25    varchar(2)      0   
                            uw_dometemp    , --  real      NULL,      -- upwelling IR dome temp. (K)                              Column26    varchar(9)      262.23 
                            uw_dometemp_qc , --  int       NULL,      --                                                          Column27    varchar(2)      0     
                            uvb            , --  real      NULL,      -- global UVB (milliWatts m^-2)                             Column28    varchar(8)      0.0 
                            uvb_qc         , --  int       NULL,      --                                                          Column29    varchar(2)      0     
                            par            , --  real      NULL,      -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8)      0.4 
                            par_qc         , --  int       NULL,      --                                                          Column31    varchar(2)      0     
                            netsolar       , --  real      NULL,      -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8)      0.4 
                            netsolar_qc    , --  int       NULL,      --                                                          Column33    varchar(2)      0   
                            netir          , --  real      NULL,      -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8)      -33.6 
                            netir_qc       , --  int       NULL,      --                                                          Column35    varchar(2)      0   
                            totalnet       , --  real      NULL,      -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8)      -33.2 
                            totalnet_qc    , --  int       NULL,      --                                                          Column37    varchar(2)      0   
                            temp           , --  real      NULL,      -- 10-meter air temperature (?C)                            Column38    varchar(8)      -10.4 
                            temp_qc        , --  int       NULL,      --                                                          Column39    varchar(2)      0    
                            rh             , --  real      NULL,      -- relative humidity (%)                                    Column40    varchar(8)      92.1 
                            rh_qc          , --  int       NULL,      --                                                          Column41    varchar(2)      0     
                            windspd        , --  real      NULL,      -- wind speed (ms^-1)                                       Column42    varchar(8)      0.8 
                            windspd_qc     , --  int       NULL,      --                                                          Column43    varchar(2)      0   
                            winddir        , --  real      NULL,      -- wind direction (degrees, clockwise from north)           Column44    varchar(8)      110.1 
                            winddir_qc     , --  int       NULL,      --                                                          Column45    varchar(2)      0   
                            pressure       , --  real      NULL,      -- station pressure (mb)                                    Column46    varchar(8)      837.1 
                            pressure_qc    , --  int       NULL       --                                                          Column47    varchar(2)      0
                            file_date      , --  datetime  NULL,      -- noaa filedate (encoded from DayOfYear)    
                            added_date       --  datetime  NULL       -- date data added to database, often delayed
                        FROM _in_SurfRad";

                        using (SqlDataReader reader = sqlReadCmd.ExecuteReader(CommandBehavior.SequentialAccess))
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    //Console.WriteLine(@"{0} {1}", reader[0], reader[13]);

                                    var noaa_year = (reader[0] as Int32?) ?? 0;                   // int       NULL,     -- year, i.e., 1995                                         Column0     varchar(5) 
                                    var noaa_jday = (reader[1] as Int32?) ?? 0;                   // int       NULL,     -- Julian day (1 through 365 [or 366])                      Column1     varchar(4) 
                                    var noaa_month = (reader[2] as Int32?) ?? 0;                   // int       NULL,     -- number of the month (1-12)                               Column2     varchar(3) 
                                    var noaa_day = (reader[3] as Int32?) ?? 0;                   // int       NULL,     -- day of the month(1-31)                                   Column3     varchar(3) 
                                    var noaa_hour = (reader[4] as Int32?) ?? 0;                   // int       NULL,     -- hour of the day (0-23)                                   Column4     varchar(3) 
                                    var noaa_min = (reader[5] as Int32?) ?? 0;                   // int       NULL,     -- minute of the hour (0-59)                                Column5     varchar(3) 
                                    var dt = (reader[6] as Decimal?) ?? 0;                 // real      NULL,     -- decimal time (hour.decimalminutes, e.g., 23.5 = 2330)    Column6     varchar(7) 
                                    var zen = (reader[7] as Decimal?) ?? 0;                 // real      NULL,     -- solar zenith angle (degrees)                             Column7     varchar(7) 
                                    var dw_solar = (reader[8] as Decimal?) ?? 0;                 // real      NULL,     -- downwelling global solar (Watts m^-2)                    Column8     varchar(8) 
                                    var dw_solar_qc = (reader[9] as Int32?) ?? 0;                   // int       NULL,     --                                                          Column9     varchar(2) 
                                    var uw_solar = (reader[10] as Decimal?) ?? 0;                // real      NULL,     -- upwelling global solar (Watts m^-2)                      Column10    varchar(8) 
                                    var uw_solar_qc = (reader[11] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column11    varchar(2) 
                                    var direct_n = (reader[12] as Decimal?) ?? 0;                // real      NULL,     -- direct-normal solar (Watts m^-2)                         Column12    varchar(8) 
                                    var direct_n_qc = (reader[13] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column13    varchar(2) 
                                    var diffuse = (reader[14] as Decimal?) ?? 0;                // real      NULL,     -- downwelling diffuse solar (Watts m^-2)                   Column14    varchar(8) 
                                    var diffuse_qc = (reader[15] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column15    varchar(2) 
                                    var dw_ir = (reader[16] as Decimal?) ?? 0;                // real      NULL,     -- downwelling thermal infrared (Watts m^-2)                Column16    varchar(8) 
                                    var dw_ir_qc = (reader[17] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column17    varchar(2) 
                                    var dw_casetemp = (reader[18] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR case temp. (K)                            Column18    varchar(9) 
                                    var dw_casetemp_qc = (reader[19] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column19    varchar(2) 
                                    var dw_dometemp = (reader[20] as Decimal?) ?? 0;                // real      NULL,     -- downwelling IR dome temp. (K)                            Column20    varchar(9) 
                                    var dw_dometemp_qc = (reader[21] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column21    varchar(2) 
                                    var uw_ir = (reader[22] as Decimal?) ?? 0;                // real      NULL,     -- upwelling thermal infrared (Watts m^-2)                  Column22    varchar(8) 
                                    var uw_ir_qc = (reader[23] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column23    varchar(2) 
                                    var uw_casetemp = (reader[24] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR case temp. (K)                              Column24    varchar(9) 
                                    var uw_casetemp_qc = (reader[25] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column25    varchar(2) 
                                    var uw_dometemp = (reader[26] as Decimal?) ?? 0;                // real      NULL,     -- upwelling IR dome temp. (K)                              Column26    varchar(9) 
                                    var uw_dometemp_qc = (reader[27] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column27    varchar(2) 
                                    var uvb = (reader[28] as Decimal?) ?? 0;                // real      NULL,     -- global UVB (milliWatts m^-2)                             Column28    varchar(8) 
                                    var uvb_qc = (reader[29] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column29    varchar(2) 
                                    var par = (reader[30] as Decimal?) ?? 0;                // real      NULL,     -- photosynthetically active radiation (Watts m^-2)         Column30    varchar(8) 
                                    var par_qc = (reader[31] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column31    varchar(2) 
                                    var netsolar = (reader[32] as Decimal?) ?? 0;                // real      NULL,     -- net solar (dw_solar - uw_solar) (Watts m^-2)             Column32    varchar(8) 
                                    var netsolar_qc = (reader[33] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column33    varchar(2) 
                                    var netir = (reader[34] as Decimal?) ?? 0;                // real      NULL,     -- net infrared (dw_ir - uw_ir) (Watts m^-2)                Column34    varchar(8) 
                                    var netir_qc = (reader[35] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column35    varchar(2) 
                                    var totalnet = (reader[36] as Decimal?) ?? 0;                // real      NULL,     -- net radiation (netsolar+netir) (Watts m^-2)              Column36    varchar(8) 
                                    var totalnet_qc = (reader[37] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column37    varchar(2) 
                                    var temp = (reader[38] as Decimal?) ?? 0;                // real      NULL,     -- 10-meter air temperature (?C)                            Column38    varchar(8) 
                                    var temp_qc = (reader[39] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column39    varchar(2) 
                                    var rh = (reader[40] as Decimal?) ?? 0;                // real      NULL,     -- relative humidity (%)                                    Column40    varchar(8) 
                                    var rh_qc = (reader[41] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column41    varchar(2) 
                                    var windspd = (reader[42] as Decimal?) ?? 0;                // real      NULL,     -- wind speed (ms^-1)                                       Column42    varchar(8) 
                                    var windspd_qc = (reader[43] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column43    varchar(2) 
                                    var winddir = (reader[44] as Decimal?) ?? 0;                // real      NULL,     -- wind direction (degrees, clockwise from north)           Column44    varchar(8) 
                                    var winddir_qc = (reader[45] as Int32?) ?? 0;                  // int       NULL,     --                                                          Column45    varchar(2) 
                                    var pressure = (reader[46] as Decimal?) ?? 0;                // real      NULL,     -- station pressure (mb)                                    Column46    varchar(8) 
                                    var pressure_qc = (reader[47] as Int32?) ?? 0;                  // int       NULL      --                                                          Column47    varchar(2) 
                                    var fileDate = (reader[48] as DateTime?) ?? DateTime.MinValue;
                                    var addedDate = (reader[49] as DateTime?) ?? DateTime.MinValue;


                                    using (SqlConnection sqlWriteConnection = new SqlConnection(strBuilderWrite.ConnectionString))
                                    {

                                        sqlWriteConnection.Open();
                                        using (SqlCommand sqlWriteCmd = sqlWriteConnection.CreateCommand())
                                        {
                                            sqlWriteCmd.CommandText = @"INSERT INTO SurfRad (
                                                noaa_year, noaa_jday, noaa_month, noaa_day, noaa_hour, noaa_min, dt, zen, dw_solar, dw_solar_qc, uw_solar, uw_solar_qc, direct_n, direct_n_qc, diffuse, diffuse_qc, dw_ir, dw_ir_qc, dw_casetemp, dw_casetemp_qc, dw_dometemp, dw_dometemp_qc, uw_ir, uw_ir_qc, uw_casetemp, uw_casetemp_qc, uw_dometemp, uw_dometemp_qc, uvb, uvb_qc, par, par_qc, netsolar, netsolar_qc, netir, netir_qc, totalnet, totalnet_qc, temp, temp_qc, rh, rh_qc, windspd, windspd_qc, winddir, winddir_qc, pressure, pressure_qc, fileDate, addedDate
                                                ) 
                                                VALUES 
                                                (
                                                @noaa_year, @noaa_jday, @noaa_month, @noaa_day, @noaa_hour, @noaa_min, @dt, @zen, @dw_solar, @dw_solar_qc, @uw_solar, @uw_solar_qc, @direct_n, @direct_n_qc, @diffuse, @diffuse_qc, @dw_ir, @dw_ir_qc, @dw_casetemp, @dw_casetemp_qc, @dw_dometemp, @dw_dometemp_qc, @uw_ir, @uw_ir_qc, @uw_casetemp, @uw_casetemp_qc, @uw_dometemp, @uw_dometemp_qc, @uvb, @uvb_qc, @par, @par_qc, @netsolar, @netsolar_qc, @netir, @netir_qc, @totalnet, @totalnet_qc, @temp, @temp_qc, @rh, @rh_qc, @windspd, @windspd_qc, @winddir, @winddir_qc, @pressure, @pressure_qc, @fileDate, @addedDate
                                                )";



                                            sqlWriteCmd.Parameters.Add("@noaa_year      ", SqlDbType.Int, 12).Value = noaa_year;
                                            sqlWriteCmd.Parameters.Add("@noaa_jday      ", SqlDbType.Int, 12).Value = noaa_jday;
                                            sqlWriteCmd.Parameters.Add("@noaa_month     ", SqlDbType.Int, 12).Value = noaa_month;
                                            sqlWriteCmd.Parameters.Add("@noaa_day       ", SqlDbType.Int, 12).Value = noaa_day;
                                            sqlWriteCmd.Parameters.Add("@noaa_hour      ", SqlDbType.Int, 12).Value = noaa_hour;
                                            sqlWriteCmd.Parameters.Add("@noaa_min       ", SqlDbType.Int, 12).Value = noaa_min;
                                            sqlWriteCmd.Parameters.Add("@dt", SqlDbType.Decimal, 16).Value = dt;
                                            sqlWriteCmd.Parameters["@dt"].Precision = 7;
                                            sqlWriteCmd.Parameters["@dt"].Scale = 3;
                                            sqlWriteCmd.Parameters.Add("@zen", SqlDbType.Decimal, 16).Value = zen;
                                            sqlWriteCmd.Parameters["@zen"].Precision = 7;
                                            sqlWriteCmd.Parameters["@zen"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_solar", SqlDbType.Decimal, 16).Value = dw_solar;
                                            sqlWriteCmd.Parameters["@dw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_solar_qc    ", SqlDbType.Int, 12).Value = dw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_solar", SqlDbType.Decimal, 16).Value = uw_solar;
                                            sqlWriteCmd.Parameters["@uw_solar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_solar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_solar_qc", SqlDbType.Int, 12).Value = uw_solar_qc;
                                            sqlWriteCmd.Parameters.Add("@direct_n", SqlDbType.Decimal, 16).Value = direct_n;
                                            sqlWriteCmd.Parameters["@direct_n"].Precision = 8;
                                            sqlWriteCmd.Parameters["@direct_n"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@direct_n_qc", SqlDbType.Int, 12).Value = direct_n_qc;
                                            sqlWriteCmd.Parameters.Add("@diffuse", SqlDbType.Decimal, 16).Value = diffuse;
                                            sqlWriteCmd.Parameters["@diffuse"].Precision = 8;
                                            sqlWriteCmd.Parameters["@diffuse"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@diffuse_qc", SqlDbType.Int, 12).Value = diffuse_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_ir", SqlDbType.Decimal, 16).Value = dw_ir;
                                            sqlWriteCmd.Parameters["@dw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@dw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@dw_ir_qc", SqlDbType.Int, 12).Value = dw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp", SqlDbType.Decimal, 16).Value = dw_casetemp;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_casetemp_qc", SqlDbType.Int, 12).Value = dw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp", SqlDbType.Decimal, 16).Value = dw_dometemp;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@dw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@dw_dometemp_qc", SqlDbType.Int, 12).Value = dw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_ir", SqlDbType.Decimal, 16).Value = uw_ir;
                                            sqlWriteCmd.Parameters["@uw_ir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uw_ir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uw_ir_qc", SqlDbType.Int, 12).Value = uw_ir_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp", SqlDbType.Decimal, 16).Value = uw_casetemp;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_casetemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_casetemp_qc", SqlDbType.Int, 12).Value = uw_casetemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp", SqlDbType.Decimal, 16).Value = uw_dometemp;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Precision = 9;
                                            sqlWriteCmd.Parameters["@uw_dometemp"].Scale = 2;
                                            sqlWriteCmd.Parameters.Add("@uw_dometemp_qc", SqlDbType.Int, 12).Value = uw_dometemp_qc;
                                            sqlWriteCmd.Parameters.Add("@uvb", SqlDbType.Decimal, 16).Value = uvb;
                                            sqlWriteCmd.Parameters["@uvb"].Precision = 8;
                                            sqlWriteCmd.Parameters["@uvb"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@uvb_qc", SqlDbType.Int, 12).Value = uvb_qc;
                                            sqlWriteCmd.Parameters.Add("@par", SqlDbType.Decimal, 16).Value = par;
                                            sqlWriteCmd.Parameters["@par"].Precision = 8;
                                            sqlWriteCmd.Parameters["@par"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@par_qc", SqlDbType.Int, 12).Value = par_qc;
                                            sqlWriteCmd.Parameters.Add("@netsolar", SqlDbType.Decimal, 16).Value = netsolar;
                                            sqlWriteCmd.Parameters["@netsolar"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netsolar"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netsolar_qc", SqlDbType.Int, 12).Value = netsolar_qc;
                                            sqlWriteCmd.Parameters.Add("@netir", SqlDbType.Decimal, 16).Value = netir;
                                            sqlWriteCmd.Parameters["@netir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@netir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@netir_qc", SqlDbType.Int, 12).Value = netir_qc;
                                            sqlWriteCmd.Parameters.Add("@totalnet", SqlDbType.Decimal, 16).Value = totalnet;
                                            sqlWriteCmd.Parameters["@totalnet"].Precision = 8;
                                            sqlWriteCmd.Parameters["@totalnet"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@totalnet_qc", SqlDbType.Int, 12).Value = totalnet_qc;
                                            sqlWriteCmd.Parameters.Add("@temp", SqlDbType.Decimal, 16).Value = temp;
                                            sqlWriteCmd.Parameters["@temp"].Precision = 8;
                                            sqlWriteCmd.Parameters["@temp"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@temp_qc", SqlDbType.Int, 12).Value = temp_qc;
                                            sqlWriteCmd.Parameters.Add("@rh", SqlDbType.Decimal, 16).Value = rh;
                                            sqlWriteCmd.Parameters["@rh"].Precision = 8;
                                            sqlWriteCmd.Parameters["@rh"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@rh_qc", SqlDbType.Int, 12).Value = rh_qc;
                                            sqlWriteCmd.Parameters.Add("@windspd", SqlDbType.Decimal, 16).Value = windspd;
                                            sqlWriteCmd.Parameters["@windspd"].Precision = 8;
                                            sqlWriteCmd.Parameters["@windspd"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@windspd_qc", SqlDbType.Int, 12).Value = windspd_qc;
                                            sqlWriteCmd.Parameters.Add("@winddir", SqlDbType.Decimal, 16).Value = winddir;
                                            sqlWriteCmd.Parameters["@winddir"].Precision = 8;
                                            sqlWriteCmd.Parameters["@winddir"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@winddir_qc", SqlDbType.Int, 12).Value = winddir_qc;
                                            sqlWriteCmd.Parameters.Add("@pressure", SqlDbType.Decimal, 16).Value = pressure;
                                            sqlWriteCmd.Parameters["@pressure"].Precision = 8;
                                            sqlWriteCmd.Parameters["@pressure"].Scale = 1;
                                            sqlWriteCmd.Parameters.Add("@pressure_qc", SqlDbType.Int, 12).Value = pressure_qc;

                                            //Console.WriteLine(@"ready to write to _clean {0} {1} {2} {3}", accountKey, DOB, age, ageAsOfDay);
                                            //sqlWriteCmd.CommandTimeout = 0; // for debugging only ... turns out the problem lied with nulls and nvarchar vs varchar
                                            sqlWriteCmd.ExecuteNonQuery();
                                            //Console.WriteLine(@"wrote to _clean {0} {1} {2} {3}", accountKey, DOB, age, ageAsOfDay);

                                        }
                                        sqlWriteConnection.Close();
                                    }
                                }
                            }
                        }
                    }
                    sqlReadConnection.Close();

                    // values needed for aggregator after work complete                   
                    etlEndTime = DateTime.Now;
                    nRecords = Counter(tableName);
                    if (!Aggregator(etlStartTime, etlEndTime, tableName, nRecords, argNoaaIncrementalDate))
                    {
                        throw new System.Exception("error encountered during Aggregator");
                    }

                }



            }
            catch (System.Exception e)
            {
                Console.WriteLine("    Caught an exception: " + e.Source + " " + e.Message + " " + e.StackTrace);
                return false;
            }
            return true;
        }





        /// <summary>
        ///  Aggregator utilizes the SQL Server Always Encrypted libraries in .Net 4.6.1 to load a stats data table.
        ///  This will track aggregated statistics of the data over time as data is loaded from NOAA
        /// </summary>
        /// <returns></returns>
        static bool Aggregator(DateTime etlStartTime, DateTime etlEndTime, string tableName, int nRecords, DateTime argNoaaIncrementalDate)
        {
            try
            {
                SqlConnectionStringBuilder strBuilderWrite = new SqlConnectionStringBuilder();
                strBuilderWrite.DataSource = "127.0.0.1,31433";
                strBuilderWrite.InitialCatalog = "noaaC";
                //strBuilderRead.IntegratedSecurity = true;
                strBuilderWrite.UserID = "sa";
                strBuilderWrite.Password = "omg.passwords!";
                strBuilderWrite.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;

                using (SqlConnection sqlWriteConnection = new SqlConnection(strBuilderWrite.ConnectionString))
                {

                    sqlWriteConnection.Open();
                    using (SqlCommand sqlWriteCmd = sqlWriteConnection.CreateCommand())
                    {
                        sqlWriteCmd.CommandText = @"INSERT INTO _loadstats (
                                                etlStartTime, etlEndTime, tableName, nRecords, ageAsOfDay  
                                                ) 
                                                VALUES 
                                                (
                                                @etlStartTime, @etlEndTime, @tableName, @nRecords, @argNoaaIncrementalDate
                                                )";

                        sqlWriteCmd.Parameters.Add("@etlStartTime", SqlDbType.NVarChar, 255).Value = etlStartTime;
                        sqlWriteCmd.Parameters.Add("@etlEndTime", SqlDbType.NVarChar, 255).Value = etlEndTime;
                        sqlWriteCmd.Parameters.Add("@tableName", SqlDbType.NVarChar, 255).Value = tableName;
                        sqlWriteCmd.Parameters.Add("@nRecords", SqlDbType.Int, 255).Value = nRecords;
                        sqlWriteCmd.Parameters.Add("@argNoaaIncrementalDate", SqlDbType.DateTime, 255).Value = argNoaaIncrementalDate;

                        Console.WriteLine(@"ready to write to _loadstats {0} {1} {2} {3} {4}", etlStartTime, etlEndTime, tableName, nRecords, argNoaaIncrementalDate);
                        sqlWriteCmd.ExecuteNonQuery();

                    }
                    sqlWriteConnection.Close();

                }
            }
            catch (System.Exception e)
            {
                Console.WriteLine("    Caught an exception: " + e.Source + " " + e.Message + " " + e.StackTrace);
                return false;
            }
            return true;
        }





    }
}