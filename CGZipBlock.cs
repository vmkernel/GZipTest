using System;

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
}
