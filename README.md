# Objectives
* Develop a command line tool for compressing and decompressing files using class System.IO.Compression.GzipStream.
* The program should effectively work in a multicore environment and should be able to process files, which are bigger than total RAM size.
* Program code must be safe and robust in terms of exceptions.
* Please use only standard libraries from .NET Framework 3.5 to work with multithreading.
*  ability to stop program correctly by Ctrl-C would be a plus.
* On successful result program should return 0, otherwise 1.

Use the following command line arguments:
* compressing: GZipTest.exe compress [original file name] [archive file name]
* decompressing: GZipTest.exe decompress [archive file name] [decompressing file name]

Please send us solution source files and Visual Studio project. Briefly describe architecture and algorithms used.



# Program description
Main business-logic of the application is held within static class CGZipCompressor.

Compression mode is set by the property CGZipCompressor.CompressionMode (System.IO.Compression) and might take only two values: Compress and Decompress. In case if the mode somehow receives value that doesn't match the specified two, the program throws an exception.


## Settings
The program has the following hardcoded settings:
* Compression block size
* Maximum length of the processing queue
* Maximum lenght of the write queue
* Number of CPU cores reserved for the operating system


## Exceptions handling
In case of any exception in any thread the program sets the CGZipCompressor.IsEmergencyShutdown flag, stores the error message with some details in CGZipCompressor.EmergencyShutdownMessage property, kills all running threads and displays the error message to a user.


## Processing stages
A compressing/decompressing process includes the following three stages:
1. Reading blocks of data from an input file.
1. Processing this blocks.
1. Writing the processed blocks to an output file.

### Stage completion flags
There's three flags that informs the whole compressor class of its current state:
* s_isInputFileRead - Indicates that an input file has been read to the end and the file read thread is finished.
* s_isDataProcessingDone - Indicates that all the data blocks from an input file has been processed and all worker threads is finished.
* s_isOutputFileWritten - Indicates that all the processed data block is written to an input file and file write thread is finished.

When all of this three flags are set to true, it means that the whole process has been finished.


## Reading input and writing output files
The basic idea of this processes is queues and read/write sequence numbers. 

### File formats
For an uncompressed file everything is pretty simple - it's just a file as it is, without any alterations made by the program.
A compressed file consists of blocks described by CGZipBlock object that contains:
* Metadata: uncompressed (original) block size (to be able to preallocate a buffer for decompression) and compressed block size (to be able to read the compressed block from the file).
* Compressed data block which size is stored in the metadata.

### Queues
There are two queues:
1. Processing queue (stores block from an input file until the blocks are processed by worker threads).
1. File write queue (stores the processed blocks until they are written to an output file).

Each queue is represented by a Dictionary<Int32, Object> with block's sequence number as a key and a block of data storead as an Object. 

The final type of this Object variable depends on the selected operation mode:
* If compression mode is selected then it's byte array in the processing queue and CGZipBlock object in the file write queue.
* If decompresion mode is selected then it's GZipBlock in the processing queue and byte array in the file write queue.

The length of each queue is limited by s_maxProcessingQueueLength and s_maxWriteQueueLength variables in order to prevent RAM overflow. When one of these limits is reached the file read thread suspends read operations until both of the queues have at least one free slot for a data block.

Every item in the queues consists of the data block and its sequence number which is assigned to a block when it's read from an input file. The sequence number remains with the block until it's written to an output file. 
All blocks of an input file are read to the processing queue. From the processing queue each block is picked up by a block processing thread, processed (compressed/decompressed) and put to the block write queue. From the write queue each processed block is picked up by the file writer thread and written to an output file.

### Sequence numbers
Each block of data is read from an input file and written to an output file sequentionally. To prevent messing with the blocks' sequence and write all the processed blocks to an output file in the same order as they were read from an input file, there are two sequence numbers:
* Read sequence number - a unique counter that starts from zero, assigns to each data block as it's read from an input file and increments after each read operations.
* Write sequence number - a unique counter that starts from zero and is incremented as a block of processed data with the same sequence number has been written to an output file.

If the write sequence number is X and there's no processed block with the same sequence number in the file write queue, the file write thread will wait untill a block with the sequence number X is placed to the queue. After that the file write thread will pick up the block, write it to an output file and increment the write sequence number. And the loop will countinue until the last block of proccessed data will be written to an output file.


## Multihreading
Besides main thread that spawns by default from main() the program spawns the following threads:
* Input file read
* Output file write
* Worker threads
* Worker threads dispatcher

#### Input file read thread
A single thread that sequentially reads blocks of data from a specified input file and places the blocks to the processing queue. Each block gets a unique sequential number which identifies the block.

The thread pauses reading operations in the following situations:
* when the file write thread resets the Block Write Queue Ready signal in order to keep the file write queue's lenght less than maximum allowed value.
* when the lenght of the queue exceeds the maximum allowed value the thread pauses read operations until a block processing thread picks up a block from the queue.

Depending on the selected operations mode the behaviour of the thread varies in the following fashion:
* Compression: the thread reads each block with the predefined size and puts it to the processing queue as a byte array.
* Decompression: before reading each block the thread reads its metadata (decompressed and compressed block size) from an input compressed file, allocates a buffer required to read the compresissed block, and reads the block to the buffer. Finally the thread puts the compressed block with its metadata to the processing queue as a CGZipBlock object.

#### Output file write thread
A single thread that sequentially writes processed blocks of data from the block write queue to the specified output file according to the blocks' sequence numbers.

The thread looks up a block with the subsequent write sequence number in the block write queue. If there's no such block the thread waits until a block processing thread signals that a new block of data has been processed. After receiving the signal the thread does the lookup once again until the block with the correct write sequence number is placed to the block write queue by a block processing thread.

The thread controls lenght of the block write queue. When the lenght exceeds maximum allowed value the thread signals to the file read thread that is must suspend any further read operations. After the lenght of the queue goes below the maxumum the thread signals to the file tead thread to resume read operations.

Depending on the selected operations mode the behaviour of the thread varies in the following fashion:
* Compression: picks up compressed data and its metadata as a CGZipBlock object from the block write queue, converts it to a byte array and writes to the output file.
* Decompression: picks up decompressed data as a byte array from the block write queue and writes to the output file.

#### Worker threads dispatcher thread
A subgke tgread that starts worker thread(s), cleans up finished worker thread(s), limits worker thread(s) number according to predefined settings and kills worker threads in case of emergency shutdown.

#### Worker thread(s)
One or more thread(s) which either compress or decompress a block of data depending on the selected compression mode.

The number of the threads depends on the number of CPU cores in a system which runs the program. Depending on the program's settings the number might be lowered if the corresponding settings is set to reserve one (or more) CPU core(s) for an operating system which runs the program.

Depending on the selected operations mode the behaviour of the thread varies in the following fashion:
* Compression: picks up an uncompressed data block as a byte array from the block processing queue, compresses it, transforms to CGZipBlock object, adds metadata and puts the block to the block write queue.
* Decompression: picks up a compressed CGZipBlock object from the block processing queue, allocates a buffer for decompressed data according to the size that is stored in metadata, decompresses the block and puts to the block write queue.

#### Inter-thread communication
There are three signals which are used for communications betwen threads
* **A block of processed data has been written to an output file**. Fires when the file writer thread completes write operation of a data block. The signal is used to resume file read thread when it is suspended because the lenght of the processing queue has reached its maximum.
* **The block write queue is ready to receive a new block**. The signal is used for throttling input file read when the block write queue becomes longer than the maximum allowed value. It is set when the length of the block write queue is less than the allowed maximum and reset when the lenght the queue is greather than the allowed maximum.
* **A block of data has been processed**. Fires when a block processing thread has finished processing a block. The signal is used to notify the file write thread that a new block of data is ready when the thread is suspended and waits for a block with correct write sequence number.
