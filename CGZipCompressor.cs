using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Threading;

// TODO: fix issue with incorrect compressed block detection (probably with block length section in the compressed file or in file's metadata)
// TODO: add file format check in order to prevent decompression of a uncompressed file

namespace GZipTest
{
    public class CGZipBlock
    {
        // Size (in bytes) of buffer which is used to store size of uncompressed data block
        private static Int32 MetadataUncompressedBlockSizeBufferLength
        {
            get
            {
                return sizeof(Int32); // TODO: replace with relative sizeof calculation
            }
        }

        // Size (in bytes) of buffer which is used to store size of compressed data block
        private static Int32 MetadataCompressedBlockSizeBufferLength
        {
            get
            {
                return sizeof(Int32); // TODO: replace with relative sizeof calculation
            }
        }

        // Size (in bytes) of metadata
        public static Int32 MetadataSize
        {
            get
            {
                return MetadataCompressedBlockSizeBufferLength + MetadataUncompressedBlockSizeBufferLength;
            }
        }

        // Size (in bytes) of an original (uncompressed) block of data
        public Int32 DataSizeUncompressed { get; set; }

        // Size (in bytes) of a compressed block of data
        public Int32 DataSizeCompressed
        {
            get
            {
                return Data.Length;
            }
        }

        // Block of GZip-compressed data without any metadata
        private Byte[] s_data;
        public Byte[] Data
        {
            get
            {
                if (s_data == null)
                {
                    s_data = new Byte[0];
                }
                return s_data;
            }

            set
            {
                if (value == null)
                {
                    s_data = new Byte[0];
                }
                s_data = value;
            }
        }

        public CGZipBlock()
        {

        }
        public CGZipBlock(Byte[] buffer)
        {
            // TODO: refactor the constructor
            /*
            Int32 originalSizeBufferSize = sizeof(Int32); // TODO: replace with relative sizeof calculation
            Int32 compressedSizeBufferSize = sizeof(Int32); // TODO: replace with relative sizeof calculation

            Byte[] originalSizeBuffer = new Byte[originalSizeBufferSize];
            Byte[] compressedSizeBuffer = new Byte[compressedSizeBufferSize];

            Array.Copy(buffer, 0, originalSizeBuffer, 0, originalSizeBufferSize);
            Array.Copy(buffer, originalSizeBufferSize, compressedSizeBuffer, 0, compressedSizeBufferSize);

            SGZipCompressedBlockInfo metadata = new SGZipCompressedBlockInfo();
            metadata.OriginalSize = BitConverter.ToInt32(originalSizeBuffer, 0);
            metadata.CompressedSize = BitConverter.ToInt32(compressedSizeBuffer, 0);
            Metadata = metadata;

            Int32 dataBufferSize = buffer.Length - originalSizeBufferSize - compressedSizeBufferSize;
            Data = new Byte[dataBufferSize];
            Array.Copy(buffer, originalSizeBufferSize + compressedSizeBufferSize, Data, 0, dataBufferSize);
            */
        }

        // Converts metadata and compressed data to byte array (to be able to write it to an output file)
        public Byte[] ToByteArray()
        {
            Byte[] uncompressedSizeBuffer = BitConverter.GetBytes(DataSizeUncompressed);
            Byte[] compressedSizeBuffer = BitConverter.GetBytes(DataSizeCompressed);

            Int32 resultantBufferLength = uncompressedSizeBuffer.Length + compressedSizeBuffer.Length + Data.Length;
            Byte[] resultantBuffer = new Byte[resultantBufferLength];

            Array.Copy(uncompressedSizeBuffer, 0, resultantBuffer, 0, uncompressedSizeBuffer.Length);
            Array.Copy(compressedSizeBuffer, 0, resultantBuffer, uncompressedSizeBuffer.Length, compressedSizeBuffer.Length);
            Array.Copy(Data, 0, resultantBuffer, uncompressedSizeBuffer.Length + compressedSizeBuffer.Length, Data.Length);

            return resultantBuffer;
        }

        // Initialize metadata from byte buffer that represents the metadata which has been read from a compressed file
        public void InitializeWithMetadata(Byte[] metadataBuffer)
        {
            // Allocating buffers for comversion from byte array to integer value
            Byte[] uncompressedBuffer = new Byte[MetadataUncompressedBlockSizeBufferLength];
            Byte[] compressedBuffer = new Byte[MetadataCompressedBlockSizeBufferLength];

            // Splitting metadata buffer in pieces of uncompressed and compressed block sizes
            Array.Copy(metadataBuffer, 0, uncompressedBuffer, 0, MetadataUncompressedBlockSizeBufferLength);
            Array.Copy(metadataBuffer, MetadataUncompressedBlockSizeBufferLength, compressedBuffer, 0, MetadataCompressedBlockSizeBufferLength);

            // Converting bytes representations of the sizes to integer
            DataSizeUncompressed = BitConverter.ToInt32(uncompressedBuffer, 0);
            Int32 dataSizeCompressed = BitConverter.ToInt32(compressedBuffer, 0);

            // Allocating compressed data buffer according to compressed data size
            s_data = new Byte[dataSizeCompressed];
        }
    }

    public static class CGZipCompressor
    {
        #region HARDCODED SETTINGS
        // HARDCODE!
        // Data block size
        // When compressing: size of data buffer per each thread
        // When decompressing: size of read buffer for compressed file
        // WARNING: block decompression thread assumes that this is the maximum possible size of the output buffer
        private static readonly Int32 s_chunkSize = 128 * 1024 * 1024;

        // HARDCODE!
        // Maximum lenght of read queue.
        // After reaching this value file read thread will wait until data from read queue will be processed by worker thread(s)
        private static readonly Int32 s_maxReadQueueLength = 10;

        // HARDCODE!
        // Maximum lenght of write queue. 
        // After reaching this value file read thread will wait until data from write queue will be written to output file
        private static readonly Int32 s_maxWriteQueueLength = 4;

        // HARDCODE!
        // Mamimum threads limiter subtraction value (useful when you don't want to hung your PC during (de)compression)
        private static readonly Int32 s_maxThreadsSubtraction = 1;
        #endregion

        #region FIELDS
        // First three bytes of a GZip file (compressed block) signature
        //private static readonly Byte[] s_gZipFileSignature = new Byte[] { 0x1F, 0x8B, 0x08 };

        #region Emergency shutdown
        // Emergency shutdown flag
        private static Boolean s_isEmergencyShutdown;
        public static Boolean IsEmergencyShutdown
        {
            get
            {
                return s_isEmergencyShutdown;
            }
        }

        // Emergency shutdown message
        private static String s_emergencyShutdownMessage;
        public static String EmergenceShutdownMessage
        {
            get
            {
                if (String.IsNullOrEmpty(s_emergencyShutdownMessage))
                {
                    s_emergencyShutdownMessage = "";

                }
                return s_emergencyShutdownMessage;
            }
        }
        #endregion

        #region Threads
        #region Worker threads
        // Threads pool
        private static Dictionary<Int64, Thread> s_workerThreads;
        private static Dictionary<Int64, Thread> WorkerThreads
        {
            get
            {
                if (s_workerThreads == null)
                {
                    s_workerThreads = new Dictionary<Int64, Thread>();
                }
                return s_workerThreads;
            }

            set
            {
                if (s_workerThreads == null)
                {
                    s_workerThreads = new Dictionary<Int64, Thread>();
                }
                if (value == null)
                {
                    s_workerThreads.Clear();
                }
                else
                {
                    s_workerThreads = value;
                }
            }
        }

        // Threads pool locker
        private static readonly Object s_workerThreadsLocker = new Object();

        // Maximum worker threads count
        private static Int32 s_maxThreadsCount;
        #endregion

        // Worker threads manager thread
        private static Thread s_workerThreadsManagerThread;

        // Input (un)compressed file read thread
        private static Thread s_inputFileReadThread;

        // Output (de)compressed file write thread
        private static Thread s_outputFileWriteThread;
        #endregion

        #region Block sequences management
        // Read sequence number
        private static Int64 s_readSequenceNumber;

        // Read sequence number
        private static Int64 s_writeSequenceNumber;
        // Write sequence number lock object
        private static readonly Object s_writeSequenceNumberLocker = new Object();
        #endregion

        #region Queues
        #region Compression queue
        // Stores uncompressed blocks that has been read from the uncompressed input file until they are picked up by Worker threads
        private static Dictionary<Int64, Byte[]> s_queueCompression;
        private static Dictionary<Int64, Byte[]> QueueCompression
        {
            get
            {
                if (s_queueCompression == null)
                {
                    s_queueCompression = new Dictionary<Int64, Byte[]>();
                }
                return s_queueCompression;
            }

            set
            {
                if (s_queueCompression == null)
                {
                    s_queueCompression = new Dictionary<Int64, Byte[]>();
                }
                if (value == null)
                {
                    s_queueCompression.Clear();
                }
                else
                {
                    s_queueCompression = value;
                }
            }
        }

        // Read queue lock object
        private static readonly Object s_queueCompressionLocker = new Object();
        #endregion

        #region Decompression queue
        // Stores compressed blocks with metadata that has been read from the input file until they are picked up by a Decompression thread
        private static Dictionary<Int64, CGZipBlock> s_queueDecompression;
        private static Dictionary<Int64, CGZipBlock> QueueDecompression
        {
            get
            {
                if (s_queueDecompression == null)
                {
                    s_queueDecompression = new Dictionary<Int64, CGZipBlock>();
                }
                return s_queueDecompression;
            }

            set
            {
                if (s_queueDecompression == null)
                {
                    s_queueDecompression = new Dictionary<Int64, CGZipBlock>();
                }
                if (value == null)
                {
                    s_queueDecompression.Clear();
                }
                else
                {
                    s_queueDecompression = value;
                }
            }
        }

        // Read queue lock object
        private static readonly Object s_queueDecompressionLocker = new Object();
        #endregion

        #region Decompressed blocks write queue
        // Stores decompressed blocks which are produced by Worker threads until they are picked up by output file write thread
        private static Dictionary<Int64, Byte[]> s_queueDecompressedWrite;
        private static Dictionary<Int64, Byte[]> QueueDecompressedWrite
        {
            get
            {
                if (s_queueDecompressedWrite == null)
                {
                    s_queueDecompressedWrite = new Dictionary<Int64, Byte[]>();
                }
                return s_queueDecompressedWrite;
            }

            set
            {
                if (s_queueDecompressedWrite == null)
                {
                    s_queueDecompressedWrite = new Dictionary<Int64, Byte[]>();
                }
                if (value == null)
                {
                    s_queueDecompressedWrite.Clear();
                }
                else
                {
                    s_queueDecompressedWrite = value;
                }
            }
        }

        // Read buffer lock object
        private static readonly Object s_queueDecompressedWriteLocker = new Object();
        #endregion

        #region Compressed blocks write queue
        // Stores processed (compressed/decompressed) blocks which are produced by Worker threads until they are picked up by output file write thread
        private static Dictionary<Int64, CGZipBlock> s_queueCompressedWrite;
        private static Dictionary<Int64, CGZipBlock> QueueCompressedWrite
        {
            get
            {
                if (s_queueCompressedWrite == null)
                {
                    s_queueCompressedWrite = new Dictionary<Int64, CGZipBlock>();
                }
                return s_queueCompressedWrite;
            }

            set
            {
                if (s_queueCompressedWrite == null)
                {
                    s_queueCompressedWrite = new Dictionary<Int64, CGZipBlock>();
                }
                if (value == null)
                {
                    s_queueCompressedWrite.Clear();
                }
                else
                {
                    s_queueCompressedWrite = value;
                }
            }
        }

        // Read buffer lock object
        private static readonly Object s_queueCompressedWriteLocker = new Object();
        #endregion
        #endregion

        #region Operations mode
        // Operations mode (compression or decompression)
        private static CompressionMode s_compressionMode;
        public static CompressionMode CompressionMode
        {
            get
            {
                return s_compressionMode;
            }
            set
            {
                s_compressionMode = value;
            }
        }
        #endregion

        #region Flags
        // Flag: the input file has been read and the file read thread has exited
        private static Boolean s_isInputFileRead;

        // Flag: all input data has been processed and all worker threads are terminated
        private static Boolean s_isDataProcessingDone;

        // Flag: the output file has been written and file write thread has exited
        private static Boolean s_isOutputFileWritten;
        #endregion

        #region Inter-thread communication signals (wait handles)
        // Event: output data queue is ready to receive new block of processed data
        // Is used to throttle input file read when output file write queue becames to loong
        private static ManualResetEvent s_signalOutputDataQueueReady = new ManualResetEvent(false);

        // Event: a data processing thread finished processing its block of data
        private static readonly ManualResetEvent s_signalWorkerThreadReady = new ManualResetEvent(false);
        // Locker object for s_signalWorkerThreadReady
        private static readonly Object s_workerThreadReadySignalLocker = new Object();

        // Event: a worker thread has been terminated
        private static readonly EventWaitHandle s_signalWorkerThreadExited = new EventWaitHandle(false, EventResetMode.AutoReset);
        // Locker object for s_signalWorkerThreadExited
        private static readonly Object s_WorkerTreadExitedSignalLocker = new Object();

        // Event: a block of output data has been written to the output file
        private static EventWaitHandle s_signalOutputDataWritten = new EventWaitHandle(false, EventResetMode.AutoReset);
        #endregion
        #endregion

        #region THREADS DEFINITION
        // Uncompressed file read function (threaded)
        private static void FileReadToCompressThread(object parameter)
        {
            s_isInputFileRead = false;

            if (s_isEmergencyShutdown)
            {
                return;
            }

            try
            {
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Input uncompressed file path for the File Read thread is null");
                }
                String fileName = (String)parameter;

                using (FileStream inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                {
                    Int64 bytesRead;
                    Int64 bufferSize;

                    while (inputStream.Position < inputStream.Length)
                    {
                        // Throtling read of thre file until the file writer thread signals that output data queue is ready to receive more data
                        s_signalOutputDataQueueReady.WaitOne();

                        // Throttling read of the input file in order to not to drain free memory
                        // If the read queue lenght is greather than maximum allowed value
                        Int32 readQueueItemsCount;
                        lock (s_queueCompressionLocker)
                        {
                            readQueueItemsCount = QueueCompression.Count;
                        }
                        if (readQueueItemsCount >= s_maxReadQueueLength)
                        {
                            // Until a block of data has been written to the output file
                            s_signalOutputDataWritten.WaitOne();

                            // And re-evaluate this contidition
                            continue;
                        }

                        // Calculating read block size
                        if ((inputStream.Length - inputStream.Position) < s_chunkSize)
                        {
                            bufferSize = (Int32)(inputStream.Length - inputStream.Position);
                        }
                        else
                        {
                            bufferSize = s_chunkSize;
                        }

                        // Is block size correct?
                        if (bufferSize <= 0)
                        {
                            throw new IndexOutOfRangeException("Current position in input stream is beyond the end of the file");
                        }

                        // Allocating read buffer and reading a block from the file
                        Byte[] buffer = new Byte[bufferSize];
                        bytesRead = inputStream.Read(buffer, 0, buffer.Length);
                        if (bytesRead <= 0)
                        {
                            throw new InvalidDataException("An attemp to read from input stream file has returned no data");
                        }

                        lock (s_queueCompressionLocker)
                        {
                            QueueCompression.Add(s_readSequenceNumber, buffer);
                        }

                        s_readSequenceNumber++;
                    }
                }
            }
            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Uncompressed File Read thread caused the process to stop: {0}", ex.Message);
            }

            s_isInputFileRead = true;
        }

        // Compressed file read function (threaded)
        private static void FileReadToDecompressThread(object parameter)
        {
            s_isInputFileRead = false;

            if (s_isEmergencyShutdown)
            {
                return;
            }

            try
            {
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Input compressed file path for File Read thread is null");
                }
                String fileName = (String)parameter;

                using (FileStream inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                {
                    while (inputStream.Position < inputStream.Length)
                    {
                        // Throtling read of thre file until the file writer thread signals that output data queue is ready to receive more data
                        s_signalOutputDataQueueReady.WaitOne();

                        // Throttling read of the input file in order to not to drain free memory
                        // If the read queue lenght is greather than maximum allowed value
                        Int32 decompressionQueueItemsCount;
                        lock (s_queueDecompressionLocker)
                        {
                            decompressionQueueItemsCount = QueueDecompression.Count;
                        }
                        if (decompressionQueueItemsCount >= s_maxReadQueueLength)
                        {
                            // Until a block of data has been written to the output file
                            s_signalOutputDataWritten.WaitOne();

                            // And re-evaluate this contidition
                            continue;
                        }

                        CGZipBlock gZipBlock = new CGZipBlock();
                        // Reading metadata
                        Byte[] metadataBuffer = new byte[CGZipBlock.MetadataSize];
                        inputStream.Read(metadataBuffer, 0, metadataBuffer.Length);
                        // TODO: check read bytes and compare with expected metadata size
                        gZipBlock.InitializeWithMetadata(metadataBuffer);

                        // Reading compressed block
                        Byte[] compressedDataBuffer = new Byte[gZipBlock.DataSizeCompressed];
                        inputStream.Read(compressedDataBuffer, 0, compressedDataBuffer.Length);
                        // TODO: check read bytes and compare with expected data size
                        gZipBlock.Data = compressedDataBuffer;

                        #region Debug
                        using (FileStream partFile = new FileStream(@"d:\tmp\GZipTest\detected_compressed_part" + s_readSequenceNumber + ".gz", FileMode.Create))
                        {
                            partFile.Write(compressedDataBuffer, 0, compressedDataBuffer.Length);
                        }
                        #endregion   

                        lock (s_queueDecompressionLocker)
                        {
                            QueueDecompression.Add(s_readSequenceNumber, gZipBlock);
                        }

                        s_readSequenceNumber++;
                    }
                }
            }
            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Compressed File Read thread caused the process to stop: {0}", ex.Message);
            }

            s_isInputFileRead = true;
        }

        // Compressed file write function (threaded)
        private static void FileWriteCompressedThread(object parameter)
        {
            s_isOutputFileWritten = false;

            try
            {
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Output file path for File Write thread is null");
                }

                String fileName = (String)parameter;

                using (FileStream outputStream = new FileStream(fileName, FileMode.Create))
                {
                    // Initial command to file read thread to start reading
                    s_signalOutputDataQueueReady.Set();

                    while (!s_isOutputFileWritten) // Don't required, but it's a good fail-safe measure
                    {
                        // Checking if there's any date in output queue
                        Int32 writeQueueItemsCount = 0;
                        lock (s_queueCompressedWriteLocker)
                        {
                            writeQueueItemsCount = QueueCompressedWrite.Count;
                        }

                        // Signalling to <X> thread
                        if (QueueCompressedWrite.Count > s_maxWriteQueueLength)
                        {
                            s_signalOutputDataQueueReady.Reset();
                        }
                        else
                        {
                            s_signalOutputDataQueueReady.Set();
                        }

                        // Suspend the thread until there's no data to write to the output file
                        if (writeQueueItemsCount <= 0)
                        {
                            if (s_isDataProcessingDone)
                            {
                                s_isOutputFileWritten = true;
                                break;
                            }
                        }

                        // Checking if there's a block of output data with the same sequence number as the write sequence number
                        Boolean isContainsWriteSequenceNumber = false;
                        lock (s_queueCompressedWriteLocker)
                        {
                            isContainsWriteSequenceNumber = QueueCompressedWrite.ContainsKey(s_writeSequenceNumber);
                        }
                        if (!isContainsWriteSequenceNumber)
                        {
                            // TODO: fix this
                            // If there's no block with correct write sequence number the the queue, wait for the next block and go round the loop
                            //s_signalWorkerThreadReady.WaitOne();
                            //lock (s_workerThreadReadySignalLocker)
                            //{
                            //    s_signalWorkerThreadReady.Reset();
                            //}
                        }
                        else
                        {
                            // If there is a block with correct write sequence number, write it to the output file
                            CGZipBlock compressedBlock;
                            lock (s_queueCompressedWriteLocker)
                            {
                                compressedBlock = QueueCompressedWrite[s_writeSequenceNumber];
                                QueueCompressedWrite.Remove(s_writeSequenceNumber);
                            }

                            // Writing a block of data with its metadata
                            Byte[] buffer = compressedBlock.ToByteArray();

                            /*
                            // DEBUG
                            CGZipBlock block = new CGZipBlock(buffer);
                            Boolean isTheSame = Array.Equals(block.Data, compressedBlock.Data);
                            */
                            
                            outputStream.Write(buffer, 0, buffer.Length);

                            #region Debug
                            using (FileStream partFile = new FileStream(@"d:\tmp\GZipTest\compressed_part" + s_writeSequenceNumber + ".gz", FileMode.Create))
                            {
                                partFile.Write(compressedBlock.Data, 0, compressedBlock.Data.Length);
                            }
                            #endregion

                            lock (s_writeSequenceNumberLocker)
                            {
                                s_writeSequenceNumber++;
                            }

                            s_signalOutputDataWritten.Set();
                        }

                        Thread.Sleep(1000);
                    }
                }
            }
            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in File Write thread caused the process to stop: {0}", ex.Message);
            }
        }

        // Decompressed file write function (threaded)
        private static void FileWriteDecompressedThread(object parameter)
        {
            s_isOutputFileWritten = false;

            try
            {
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Output file path for File Write thread is null");
                }
                String fileName = (String)parameter;

                using (FileStream outputStream = new FileStream(fileName, FileMode.Create))
                {
                    // Initial command to file read thread to start reading
                    s_signalOutputDataQueueReady.Set();

                    while (!s_isOutputFileWritten) // Don't required, but it's a good fail-safe measure
                    {
                        // Checking if there's any date in output queue
                        Int32 writeQueueItemsCount = 0;
                        lock (s_queueDecompressedWriteLocker)
                        {
                            writeQueueItemsCount = QueueDecompressedWrite.Count;
                        }

                        // Signalling to <X> thread
                        if (writeQueueItemsCount > s_maxWriteQueueLength)
                        {
                            s_signalOutputDataQueueReady.Reset();
                        }
                        else
                        {
                            s_signalOutputDataQueueReady.Set();
                        }

                        // Suspend the thread until there's no data to write to the output file
                        if (writeQueueItemsCount <= 0)
                        {
                            if (s_isDataProcessingDone)
                            {
                                s_isOutputFileWritten = true;
                                break;
                            }
                        }

                        // Checking if there's a block of output data with the same sequence number as the write sequence number
                        Boolean isContainsWriteSequenceNumber = false;
                        lock (s_queueDecompressedWriteLocker)
                        {
                            isContainsWriteSequenceNumber = QueueDecompressedWrite.ContainsKey(s_writeSequenceNumber);
                        }
                        if (!isContainsWriteSequenceNumber)
                        {
                            // TODO: fix this
                            // If there's no block with correct write sequence number the the queue, wait for the next block and go round the loop
                            //s_signalWorkerThreadReady.WaitOne();
                            //lock (s_workerThreadReadySignalLocker)
                            //{
                            //    s_signalWorkerThreadReady.Reset();
                            //}
                        }
                        else
                        {
                            // If there is a block with correct write sequence number, write it to the output file
                            Byte[] buffer;
                            lock (s_queueDecompressedWriteLocker)
                            {
                                buffer = QueueDecompressedWrite[s_writeSequenceNumber];
                                QueueDecompressedWrite.Remove(s_writeSequenceNumber);
                            }

                            outputStream.Write(buffer, 0, buffer.Length);

                            #region Debug

                            switch (s_compressionMode)
                            {
                                case CompressionMode.Compress:
                                    using (FileStream partFile = new FileStream(@"d:\tmp\compressed_part" + s_writeSequenceNumber + ".gz", FileMode.Create))
                                    {
                                        partFile.Write(buffer, 0, buffer.Length);
                                    }
                                    break;

                                case CompressionMode.Decompress:
                                    using (FileStream partFile = new FileStream(@"d:\tmp\decompressed_part" + s_writeSequenceNumber + ".bin", FileMode.Create))
                                    {
                                        partFile.Write(buffer, 0, buffer.Length);
                                    }
                                    break;

                                default:
                                    break;
                            }
                            #endregion

                            lock (s_writeSequenceNumberLocker)
                            {
                                s_writeSequenceNumber++;
                            }

                            s_signalOutputDataWritten.Set();
                        }

                        Thread.Sleep(1000);
                    }
                }
            }
            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in File Write thread caused the process to stop: {0}", ex.Message);
            }
        }

        // Threaded compression function
        private static void BlockCompressionThread(object parameter)
        {
            if (s_isEmergencyShutdown)
            {
                return;
            }

            try
            {
                // Checking and receiving thread sequence number from input object
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Thread sequence number for Block Compression thread is null");
                }
                Int64 threadSequenceNumber = (Int64)parameter;

                // Moving a block of uncompressed data from read queue to a local buffer
                byte[] buffer;
                lock (s_queueCompressionLocker)
                {
                    buffer = QueueCompression[threadSequenceNumber];
                    QueueCompression.Remove(threadSequenceNumber);
                }

                // Retry memory allocation flag in case of memory shortage exception
                Boolean isRetryMemoryAllocation = false;
                do
                {
                    try
                    {
                        // Allocating memory stream to write to which from the uncompressed data buffer
                        using (MemoryStream outputStream = new MemoryStream(buffer.Length))
                        {
                            using (GZipStream compressionStream = new GZipStream(outputStream, CompressionMode.Compress))
                            {
                                // compressing data
                                compressionStream.Write(buffer, 0, buffer.Length);
                            }
                            
                            // Placing compressed data in a special data block
                            CGZipBlock compressedBlock = new CGZipBlock();
                            compressedBlock.Data = outputStream.ToArray();
                            compressedBlock.DataSizeUncompressed = buffer.Length;

                            // Placing the block of data and metadata to the write queue
                            lock (s_queueCompressedWriteLocker)
                            {
                                QueueCompressedWrite.Add(threadSequenceNumber, compressedBlock);
                            }
                            
                        }
                    }
                    catch (OutOfMemoryException)
                    {
                        isRetryMemoryAllocation = true;
                        Thread.Sleep(10000); // TODO: replace with a signal from another thread

                    }
                } while (isRetryMemoryAllocation);


            }
            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Block Compression thread caused the process to stop: {0}", ex.Message);
            }
        }

        // Threaded decompression function
        private static void BlockDecompressionThread(object parameter)
        {
            // TODO: fix exception when trying to decompress a compressed block

            if (s_isEmergencyShutdown)
            {
                return;
            }

            try
            {
                // Checking and receiving the parameter
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Thread sequence number for Block Decompression thread is null");
                }
                Int64 threadSequenceNumber = (Int64)parameter;

                // Getting unprocessed data from read queue to internal buffer
                CGZipBlock gZipBlock;
                lock (s_queueDecompressionLocker)
                {
                    gZipBlock = QueueDecompression[threadSequenceNumber];
                    QueueDecompression.Remove(threadSequenceNumber);
                }

                // TODO: check if the block is correct (data != null, sizes > 0)

                // Retry memory allocation flag in case of OutOfMemory exception
                Boolean isRetryMemoryAllocation = false;
                do
                {
                    try
                    {
                        // Creating memory stream from unprocessed data
                        using (MemoryStream inputStream = new MemoryStream(gZipBlock.Data))
                        {
                            Int32 bytesRead;

                            // Allocating output buffer according to pre-defined chunk size
                            Byte[] outputBuffer = new Byte[gZipBlock.DataSizeUncompressed];
                            using (GZipStream gZipStream = new GZipStream(inputStream, CompressionMode.Decompress))
                            {
                                bytesRead = gZipStream.Read(outputBuffer, 0, outputBuffer.Length);
                            }
                            // TODO: compare expected and actual bytes read

                            // Putting processed data block to File Write thread
                            lock (s_queueDecompressedWriteLocker)
                            {
                                QueueDecompressedWrite.Add(threadSequenceNumber, outputBuffer);
                            }
                        }
                    }
                    catch (OutOfMemoryException)
                    {
                        // Handling OutOfMemory exception with wait and retry
                        isRetryMemoryAllocation = true;
                        Thread.Sleep(10000); // TODO: replace with a signal from another thread
                    }
                } while (isRetryMemoryAllocation);
            }

            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Block Decompression thread caused the process to stop: {0}", ex.Message);
            }
        }

        // Threaded threads dispatcher that starts worker threads and limits their number
        private static void WorkerThreadsDispatcherThread()
        {
            if (s_isEmergencyShutdown == true)
            {
                return;
            }

            Int32 readQueueCount = 0;
            try
            {
                do
                {
                    // Killing all running threads is emergency shutdown is requested
                    if (s_isEmergencyShutdown == true)
                    {
                        if (WorkerThreads.Count > 0)
                        {
                            foreach (Int32 threadSequenceNumber in WorkerThreads.Keys)
                            {
                                if (WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.Background ||
                                    WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.Running ||
                                    WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.Suspended ||
                                    WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.WaitSleepJoin)
                                {
                                    WorkerThreads[threadSequenceNumber].Abort();
                                }
                            }
                        }

                        return;
                    }

                    // Looking for finished threads
                    if (WorkerThreads.Count > 0)
                    {
                        List<Int32> finishedThreads = new List<Int32>();
                        foreach (Int32 threadSequenceNumber in WorkerThreads.Keys)
                        {
                            if (WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.Stopped ||
                                WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.Aborted)
                            {
                                finishedThreads.Add(threadSequenceNumber);
                            }
                        }

                        // If any finished threads has been found
                        foreach (Int32 threadSequenceNumber in finishedThreads)
                        {
                            // Removing all of them
                            WorkerThreads.Remove(threadSequenceNumber);
                        }
                    }

                    // If there's less than maximum allowed threads are running, spawn a new one                    
                    if (WorkerThreads.Count < s_maxThreadsCount)
                    {
                        switch (s_compressionMode)
                        {
                            case CompressionMode.Compress:
                                lock (s_queueCompressionLocker)
                                {
                                    foreach (Int64 threadSequenceNumber in QueueCompression.Keys)
                                    {
                                        if (!WorkerThreads.ContainsKey(threadSequenceNumber))
                                        {
                                            Thread workerThread = new Thread(BlockCompressionThread);
                                            workerThread.Name = String.Format("Block compression (seq: {0})", threadSequenceNumber);
                                            WorkerThreads.Add(threadSequenceNumber, workerThread);
                                            WorkerThreads[threadSequenceNumber].Start(threadSequenceNumber);
                                            break;
                                        }
                                    }
                                }
                                break;

                            case CompressionMode.Decompress:
                                lock (s_queueDecompressionLocker)
                                {
                                    foreach (Int64 threadSequenceNumber in QueueDecompression.Keys)
                                    {
                                        if (!WorkerThreads.ContainsKey(threadSequenceNumber))
                                        {
                                            // Spawn a corresponding thread according to the selected operations mode
                                            Thread workerThread = new Thread(BlockDecompressionThread);
                                            workerThread.Name = String.Format("Block decompression (seq: {0})", threadSequenceNumber);
                                            WorkerThreads.Add(threadSequenceNumber, workerThread);
                                            WorkerThreads[threadSequenceNumber].Start(threadSequenceNumber);
                                            break;
                                        }
                                    }
                                }
                                break;

                            default:
                                throw new Exception("Unknown operations mode is specified");
                        }                        
                    }
                    else
                    {
                        // If the limit of running worker thread is reached
                        // Stop spawning new threads and wait for the running ones to finish
                        Thread.Sleep(1000);
                    }

                    // Check if there's any block of data in the file read queue
                    lock (s_queueCompressionLocker)
                    {
                        readQueueCount = QueueCompression.Count;
                    }

                } while (!s_isInputFileRead ||
                         WorkerThreads.Count > 0 ||
                         readQueueCount > 0);
            }
            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Worker Threads Dispatcher thread caused the process to stop: {0}", ex.Message);
            }

            s_isDataProcessingDone = true;
        }
        #endregion

        #region OTHER FUNCTIONS / PROCEDURES
        // Initialize internal variables
        private static void Initialize()
        {
            if (s_isEmergencyShutdown)
            {
                return;
            }

            try
            {
                // Initializing threads
                s_workerThreadsManagerThread = null;
                s_inputFileReadThread = null;
                s_outputFileWriteThread = null;

                // Initializing flags
                s_isInputFileRead = false;
                s_isDataProcessingDone = false;
                s_isOutputFileWritten = false;
                s_isEmergencyShutdown = false;

                // Cleaning emergency shutdown message
                s_emergencyShutdownMessage = "";

                // Resetting sequence numbers
                s_readSequenceNumber = 0;
                s_writeSequenceNumber = 0;

                // Evaluating maximum threads count
                s_maxThreadsCount = Environment.ProcessorCount - s_maxThreadsSubtraction;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception during compression module initialization caused the process to stop: {0}", ex.Message);
            }
        }

        // Execute main compression / decompression logic
        public static void Run(String inputFilePath, String outputFilePath)
        {
            try
            {
                Initialize();

                if (s_isEmergencyShutdown)
                {
                    return;
                }

                // Starting compression threads manager thread
                s_workerThreadsManagerThread = new Thread(WorkerThreadsDispatcherThread);
                s_workerThreadsManagerThread.Name = "Worker threads manager";
                s_workerThreadsManagerThread.Start();

                // Initializing input file read thread
                switch (s_compressionMode)
                {
                    case CompressionMode.Compress:
                        // Initializing file reader thread
                        s_inputFileReadThread = new Thread(FileReadToCompressThread);
                        s_inputFileReadThread.Name = "Uncompressed file reader";

                        // Initializing file writer thread
                        s_outputFileWriteThread = new Thread(FileWriteCompressedThread);
                        s_outputFileWriteThread.Name = "Compressed file writer";
                        break;

                    case CompressionMode.Decompress:
                        // Starting file reader thread
                        s_inputFileReadThread = new Thread(FileReadToDecompressThread);
                        s_inputFileReadThread.Name = "Read compressed input file";

                        // Initializing file writer thread
                        s_outputFileWriteThread = new Thread(FileWriteDecompressedThread);
                        s_outputFileWriteThread.Name = "Decompressed file writer";
                        break;

                    default:
                        throw new Exception("Unknown operations mode is specified");
                }

                // Starting previously initialized input file read thread
                s_inputFileReadThread.Start(inputFilePath);

                // Starting compressed file write thread
                s_outputFileWriteThread.Start(outputFilePath);

                // Awaiting for all thread to complete while being able to kill them if an exception is occured
                while (!s_isInputFileRead ||
                       !s_isDataProcessingDone ||
                       !s_isOutputFileWritten)
                {
                    // Killing all the threads that was started here if emergency shutdown is requested
                    if (s_isEmergencyShutdown)
                    {
                        if (s_inputFileReadThread != null)
                        {
                            s_inputFileReadThread.Abort();
                        }

                        if (s_outputFileWriteThread != null)
                        {
                            s_outputFileWriteThread.Abort();
                        }

                        if (s_workerThreadsManagerThread != null)
                        {
                            s_workerThreadsManagerThread.Abort();
                        }

                        break;
                    }

                    Thread.Sleep(10000);
                }
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in compression method caused the process to stop: {0}", ex.Message);
            }
        }
        #endregion
    }
}
