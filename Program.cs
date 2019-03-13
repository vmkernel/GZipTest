using System;
using System.IO;
using System.IO.Compression;

namespace GZipTest
{
    class Program
    {
        static void PrintUsage()
        {
            String appName;
            if (String.IsNullOrEmpty(System.AppDomain.CurrentDomain.FriendlyName))
            {
                appName = "app.exe";
            }
            else
            {
                appName = System.AppDomain.CurrentDomain.FriendlyName;
            }
            String message = String.Format(
                "\nThis program implements multi-threaded compresses and decompresses for a specified file using GZipStream class from .NET 3.5\n\n" +
                "Usage:\n" +
                "{0} MODE SOURCE DESTINATION\n\n" +
                "MODEs:\n" +
                "  compress – pack a file\n" + 
                "    SOURCE - original file path\n" +
                "    DESTINATION - compressed file path\n\n" + 
                "  decompress – unpack an archive\n" + 
                "    SOURCE - compressed file path\n" +
                "    DESTINATION - decompressed file path\n\n" + 
                "Examples:\n" +
                "{0} compress \"c:\\Documents\\iis.log\" \"c:\\Documents\\iis.log.gz\"\n" +
                "{0} decompress \"c:\\Documents\\iis.log.gz\" \"c:\\Documents\\iis.log\"\n\n",
                appName);

            Console.Write(message);
        }

        static int Main(string[] args)
        {
            String inputFilePath;
            String outputFilePath;

            try
            {
                #region Checking arguments
                // If "help" is requested
                if (args != null &&
                    args.Length > 0 &&
                    String.Compare(args[0], "help", true) == 0)
                {
                    PrintUsage();
                    return 0;
                }

                if (args == null ||
                    args.Length != 3)
                {
                    // If less or more than 3 arguments are specefied, print an error message
                    String message = String.Format("Incorrect number of parameters (expected: 3, got {0})\n", args.Length);
                    throw new ArgumentException(message);
                }

                // Checking operations mode switch
                String mode = args[0];
                if (String.Compare(mode, "compress", true) == 0)
                {
                    // Setting compression mode
                    Console.WriteLine("Compression mode is specified.");
                    CGZipCompressor.CompressionMode = CompressionMode.Compress;
                }
                else if (String.Compare(mode, "decompress", true) == 0)
                {
                    // Setting decompression mode
                    Console.WriteLine("Decompression mode is specified.");
                    CGZipCompressor.CompressionMode = CompressionMode.Decompress;
                }
                else
                {
                    // Unknown mode is spceified
                    String message = String.Format("Incorrect mode specified (expected: \"compress\" or \"decompress\" or \"help\", got \"{0}\")\n", mode);
                    throw new ArgumentException(message);
                }

                // Checking input file
                inputFilePath = args[1];
                if (!File.Exists(inputFilePath))
                {
                    // Input file must exists
                    String message = String.Format("Can't find the specified input file \"{0}\"\n", inputFilePath);
                    throw new ArgumentException(message);
                }

                outputFilePath = args[2];
                // TODO: uncomment
                /* DEBUG
                if (File.Exists(outputFilePath))
                {
                    // Output file mustn't exists
                    String message = String.Format("The specified output file is already exists: \"{0}\"\n", outputFilePath);
                    throw new ArgumentException(message);
                }
                */
                #endregion

                #region Starting compression
                Console.WriteLine("Working...");
                CGZipCompressor.Run(inputFilePath, outputFilePath);

                if (CGZipCompressor.IsEmergencyShutdown)
                {
                    String message = String.Format("The compression process was aborted because of the following error: {0}", CGZipCompressor.EmergenceShutdownMessage);
                    throw new Exception(message);
                }
                else
                {
                    Console.WriteLine("The process has been completed successfully");
                    Console.WriteLine("\nPress ENTER to exit...");
                    Console.ReadLine();

                    return 0;
                }
                #endregion
            }
            catch (Exception ex)
            {
                String message = String.Format("\nERROR: {0}", ex.Message);
                Console.WriteLine(message);
                Console.WriteLine("\nPress ENTER to exit...");
                Console.ReadLine();

                return 1;
            }
        }
    }
}
