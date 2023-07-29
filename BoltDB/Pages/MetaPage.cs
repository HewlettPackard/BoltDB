/* Copyright 2023 Hewlett Packard Enterprise Development LP.
 * 
 * You can redistribute this program and/or modify it under the terms of
 * the GNU Lesser Public License version 2.1
 */
using HashDepot;
using System.IO;
using Utilities;

namespace BoltDB
{
    /// <remarks>
    /// Defined in cmd\bolt\main.go
    /// </remarks>
    internal class MetaPage : Page
    {
        public const uint Signature = 0xED0CDAED;
        public const uint SupportedVersion = 2;

        private uint Magic { get; set; }

        private uint Version { get; set; }

        public uint PageSize { get; private set; }

        public uint Flags { get; private set; }

        public BucketLocation Root { get; private set; }

        public ulong FreeList { get; private set; }

        public ulong PageID { get; private set; }

        public ulong TxID { get; private set; }

        public ulong Checksum { get; private set; }

        public MetaPage()
        {
            Magic = Signature;
            Version = SupportedVersion;
        }

        public MetaPage(byte[] page, ref int offset)
        {
            Magic = LittleEndianReader.ReadUInt32(page, ref offset);
            Version = LittleEndianReader.ReadUInt32(page, ref offset);
            PageSize = LittleEndianReader.ReadUInt32(page, ref offset);
            Flags = LittleEndianReader.ReadUInt32(page, ref offset);
            Root = BucketLocation.ReadBucketLocation(page, ref offset);
            FreeList = LittleEndianReader.ReadUInt64(page, ref offset);
            PageID = LittleEndianReader.ReadUInt64(page, ref offset);
            TxID = LittleEndianReader.ReadUInt64(page, ref offset);
            Checksum = LittleEndianReader.ReadUInt64(page, ref offset);
        }

        public bool IsValid()
        {
            if (Magic != Signature)
            {
                return false;
            }

            if (Version != SupportedVersion)
            {
                return false;
            }

            byte[] buffer = GetBytesExcludingChecksum();
            ulong calculatedChecksum = CalculateChecksum(buffer);
            return (Checksum == calculatedChecksum);
        }

        public byte[] GetBytesExcludingChecksum()
        {
            byte[] bytes = new byte[56];
            int offset = 0;
            LittleEndianWriter.WriteUInt32(bytes, ref offset, Magic);
            LittleEndianWriter.WriteUInt32(bytes, ref offset, Version);
            LittleEndianWriter.WriteUInt32(bytes, ref offset, PageSize);
            LittleEndianWriter.WriteUInt32(bytes, ref offset, Flags);
            Root.Write(bytes, ref offset);
            LittleEndianWriter.WriteUInt64(bytes, ref offset, FreeList);
            LittleEndianWriter.WriteUInt64(bytes, ref offset, PageID);
            LittleEndianWriter.WriteUInt64(bytes, ref offset, TxID);
            return bytes;
        }

        public void Write(Stream stream)
        {
            LittleEndianWriter.WriteUInt32(stream, Magic);
            LittleEndianWriter.WriteUInt32(stream, Version);
            LittleEndianWriter.WriteUInt32(stream, PageSize);
            LittleEndianWriter.WriteUInt32(stream, Flags);
            Root.Write(stream);
            LittleEndianWriter.WriteUInt64(stream, FreeList);
            LittleEndianWriter.WriteUInt64(stream, PageID);
            LittleEndianWriter.WriteUInt64(stream, TxID);
            LittleEndianWriter.WriteUInt64(stream, Checksum);

        }

        private ulong CalculateChecksum(byte[] page)
        {
            return Fnv1a.Hash64(page);
        }
    }
}
