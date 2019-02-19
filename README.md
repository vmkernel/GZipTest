# gzip-archiver

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
