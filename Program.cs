using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace GZipTest
{
    class Program
    {

        // Hardcoded variable for CPU cores count
        static int _cpuCount = 4;

        // Data buffer which is shared between threads
        static byte[][] _buffer;

        // Data buffer which is shared between threads
        static byte[][] _srcBuffer;

        // Destination data buffer which is shared between threads
        static byte[][] _dstBuffer;

        // Hardcoded size of buffer to read from file to memory
        static int _sizeofBuffer = 1024 * 1024 * 1024; // 1024M

        // Hardcoded name of source file to pack
        static String srcFileName = @"E:\Downloads\Movies\Imaginaerum.2012.1080p.BluRay.x264.YIFY.mp4";


        static void CountThread(int id)
        {
            for (UInt64 i = 0; i < UInt64.MaxValue; i++)
            {
                //Console.WriteLine(String.Format("Thread {0}: counting {1}", id, (i+1)));
                //Thread.Sleep(100);
            }
        }

        static void FileReadThread(FileStream stream, long offset, long count)
        {
            Console.WriteLine(String.Format("Got offset = {0}; length = {1}", offset, count));

            using (FileStream dstFileStream = new FileStream(@"e:\tmp\compressed-file.gz", FileMode.CreateNew))
            {
                using (GZipStream compressionStream = new GZipStream(dstFileStream, CompressionMode.Compress))
                {
                    //compressionStream.Write
                }
            }
            

            /*
            byte[] buffer = new byte[count];
            int bytesRead = stream.Read(buffer, 0, (int)count);

            Console.WriteLine(String.Format("Read {0} bytes from offset {1} to {0}", bytesRead, offset, offset + count));
            */
        }


        static int threadNumber = Environment.ProcessorCount;

        //static Thread[] tPool = new Thread[threadNumber];

        // data read from source file
        static byte[][] dataArray = new byte[threadNumber][];

        // compressed data
        static byte[][] compressedDataArray = new byte[threadNumber][];

        static int dataPortionSize = 10000000;
        static int dataArraySize = dataPortionSize * threadNumber;

        static public void Compress(string inFileName)
        {

            FileStream inFile = new FileStream(inFileName, FileMode.Open, FileAccess.Read);
            FileStream outFile = new FileStream(inFileName + ".gz", FileMode.Append);
            int _dataPortionSize;
            Thread[] tPool;
            Console.Write("Compressing...");
            while (inFile.Position < inFile.Length)
            {
                Console.Write(".");
                tPool = new Thread[threadNumber];
                for (int portionCount = 0; (portionCount < threadNumber) && (inFile.Position < inFile.Length); portionCount++)
                {
                    if (inFile.Length - inFile.Position <= dataPortionSize)
                    {
                        _dataPortionSize = (int)(inFile.Length - inFile.Position);
                    }
                    else
                    {
                        _dataPortionSize = dataPortionSize;
                    }
                    dataArray[portionCount] = new byte[_dataPortionSize];
                    inFile.Read(dataArray[portionCount], 0, _dataPortionSize);

                    tPool[portionCount] = new Thread(CompressBlock);
                    tPool[portionCount].Start(portionCount);
                }

                for (int portionCount = 0; (portionCount < threadNumber) && (tPool[portionCount] != null);)
                {
                    if (tPool[portionCount].ThreadState == ThreadState.Stopped)
                    {
                        outFile.Write(compressedDataArray[portionCount], 0, compressedDataArray[portionCount].Length);
                        portionCount++;
                    }
                }
            }

            outFile.Close();
            inFile.Close();
        }

        static public void CompressBlock(object i)
        {
            using (MemoryStream output = new MemoryStream(dataArray[(int)i].Length))
            {
                using (GZipStream cs = new GZipStream(output, CompressionMode.Compress))
                {
                    cs.Write(dataArray[(int)i], 0, dataArray[(int)i].Length);
                }
                compressedDataArray[(int)i] = output.ToArray();
            }
        }


        // Threaded compression function
        static public void BlockCompressionThread(int threadIndex)
        {
            using (MemoryStream outputStream = new MemoryStream(_srcBuffer[threadIndex].Length))
            {
                using (GZipStream compressionStream = new GZipStream(outputStream, CompressionMode.Compress))
                {
                    compressionStream.Write(_srcBuffer[threadIndex], 0, _srcBuffer[threadIndex].Length);
                }
                _dstBuffer[threadIndex] = outputStream.ToArray();
            }
        }


        static int Main(string[] args)
        {

            Compress(Program.srcFileName);

            /*
            FileInfo srcFileInfo = new FileInfo(srcFileName);
            //String dstFileName = srcFileInfo.FullName + ".gz";

            using (FileStream srcFileStream = File.Open(srcFileName, FileMode.Open, FileAccess.Read))
            {
                int bytesRead = 0;
                do
                {
                    bytesRead = srcFileStream.Read(Program._buffer, 0, Program._buffer.Length);

                    if (bytesRead > 0)
                    {
                        // Split the buffer between threads
                        long threadBufferSize = bytesRead / Program._cpuCount;
                        long firstThreadBufferSize = bytesRead - threadBufferSize * (Program._cpuCount - 1);

                        // Calculating thread count according to CPU cores count
                        List<Thread> threads = new List<Thread>();
                        long offset = 0;
                        long lenght = 0;
                        for (int i = 0; i < Program._cpuCount; i++)
                        {
                            String dstFileName = srcFileInfo.FullName + "_" + i + ".gz";
                            using (FileStream dstFileStream = File.Create(dstFileName))
                            {
                                using (GZipStream compressionStream = new GZipStream(dstFileStream, CompressionMode.Compress))
                                {
                                    //compressionStream.Write
                                }
                            }

                            /*
                            if (i == 0)
                            {
                                offset = 0;
                                lenght = firstChunkSize;
                            }
                            else
                            {
                                offset = firstChunkSize + (i - 1) * chunkSize;
                                lenght = chunkSize;
                            }
                            
                            Thread thread = new Thread(() => FileReadThread(srcFileStream, offset, lenght));
                            thread.Start();
                            threads.Add(thread);
                            
                        }
                    }

                } while (bytesRead > 0);
            }

            /*
            String srcFileName = @"E:\Downloads\Movies\Imaginaerum.2012.1080p.BluRay.x264.YIFY.mp4";
            FileInfo srcFileInfo = new FileInfo(srcFileName);

            long chunkSize = (srcFileInfo.Length / Program._cpuCount);
            long firstChunkSize = srcFileInfo.Length - chunkSize * (Program._cpuCount-1);

            using (FileStream srcFileStream = File.Open(srcFileName, FileMode.Open, FileAccess.Read))
            {
                List<Thread> threads = new List<Thread>();
                long offset = 0;
                long lenght = 0;
                for (int i = 0; i < Program._cpuCount; i++)
                {
                    if (i == 0)
                    {
                        offset = 0;
                        lenght = firstChunkSize;
                    }
                    else
                    {
                        offset = firstChunkSize + (i - 1) * chunkSize;
                        lenght = chunkSize;
                    }

                    Thread thread = new Thread(() => FileReadThread(srcFileStream, offset, lenght));
                    thread.Start();
                    threads.Add(thread);
                }


            }
            */

            /*
            Thread t0 = new Thread(() => CountThread(0));
            Thread t1 = new Thread(() => CountThread(1));
            Thread t2 = new Thread(() => CountThread(2));
            Thread t3 = new Thread(() => CountThread(2));
            t0.Start();
            t1.Start();
            t2.Start();
            t3.Start();
            */

            //String srcDirectoryPath = @"e:\Downloads\Movies\";
            //DirectoryInfo sourceDirectory = new DirectoryInfo(srcDirectoryPath);
            //sourceDirectory.GetFiles()

            /*
            String checkFileName = @"E:\Downloads\Movies\Imaginaerum.2012.1080p.BluRay.x264.YIFY (1).mp4";
            using (FileStream compressedFileStream = File.OpenRead(dstFileName))
            {
                using (GZipStream decompressionStream = new GZipStream(compressedFileStream, CompressionMode.Decompress))
                {
                    using (FileStream dstFileStream = File.Create(checkFileName))
                    {
                        int bytesRead = 0;
                        do
                        {
                            bytesRead = decompressionStream.Read(buffer, 0, buffer.Length);
                            if (bytesRead > 0)
                            {
                                dstFileStream.Write(buffer, 0, bytesRead);
                            }
                        } while (bytesRead > 0);
                    }
                }
            }
            */

            Console.ReadLine();

            return 0;
        }
    }
}
