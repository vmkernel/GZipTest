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

        static int _cpuCount = 1;

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

            byte[] buffer = new byte[count];
            int bytesRead = stream.Read(buffer, 0, (int)count);

            Console.WriteLine(String.Format("Read {0} bytes from offset {1} to {0}", bytesRead, offset, offset + count));
        }

        static int Main(string[] args)
        {

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
            


            
            //Thread t0 = new Thread(() => CountThread(0));
            //Thread t1 = new Thread(() => CountThread(1));
            //Thread t2 = new Thread(() => CountThread(2));
            //Thread t3 = new Thread(() => CountThread(2));
            //t0.Start();
            //t1.Start();
            //t2.Start();
            //t3.Start();


            Console.ReadLine();
            //String srcDirectoryPath = @"e:\Downloads\Movies\";
            //DirectoryInfo sourceDirectory = new DirectoryInfo(srcDirectoryPath);
            //sourceDirectory.GetFiles()
            /*
            int sizeofBuffer = 512 * 1024 * 1024; // 1M
            byte[] buffer = new byte[sizeofBuffer];

            String srcFileName = @"E:\Downloads\Movies\Imaginaerum.2012.1080p.BluRay.x264.YIFY.mp4";
            FileInfo srcFileInfo = new FileInfo(srcFileName);
            String dstFileName = srcFileInfo.FullName + ".gz";

            using (FileStream srcFileStream = File.Open(srcFileName, FileMode.Open, FileAccess.Read))
            {
                using (FileStream dstFileStream = File.Create(dstFileName))
                {
                    using (GZipStream compressionStream = new GZipStream(dstFileStream, CompressionMode.Compress))
                    {
                        int bytesRead = 0;
                        do
                        {
                            bytesRead = srcFileStream.Read(buffer, 0, buffer.Length);
                            if (bytesRead > 0)
                            {
                                compressionStream.Write(buffer, 0, bytesRead);
                            }

                        } while (bytesRead > 0);
                    }
                }
            }


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

            return 0;
        }
    }
}
