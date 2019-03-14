# Objectives
Develop a command line tool for compressing and decompressing files using class System.IO.Compression.GzipStream.
The program should effectively work in a multicore environment and should be able to process files, which are bigger than total RAM size.
Program code must be safe and robust in terms of exceptions.
Please use only standard libraries from .NET Framework 3.5 to work with multithreading.
An ability to stop program correctly by Ctrl-C would be a plus.

Use the following command line arguments:
compressing: GZipTest.exe compress [original file name] [archive file name]
decompressing: GZipTest.exe decompress [archive file name] [decompressing file name]

On successful result program should return 0, otherwise 1.
Please send us solution source files and Visual Studio project. Briefly describe architecture and algorithms used.


# Program description
Main business-logic of the application is held within static class CGZipCompressor.

Compression mode is set by the property CGZipCompressor.CompressionMode (System.IO.Compression) and might take only two values: Compress and Decompress. In case if the mode somehow receives another value that doesn't equal to this thow, the program will throw an exception.

## Exceptions handling
In case of any exception in any thread the program will set the IsEmergencyShutdown flag and store the exception's message along with soeme details in EmergencyShutdownMessage string variable. This message will be displayed to a user.

## Multihreading
Besides main thread that spawns by default from main() procedure the program spawns the following threads:
* A single input file read thread
* A single output file write thread
* A single worker threads dispatcher thread
* One or more worker threads (depending on the number of CPUs in the system which runs the program)

## Settings
The program has the following hardcoded settings:
* Compression block size
* Maximal length of processing queue
* Maximal lenght of write queue
* Number of CPU cores which are reserved for operating system's needs