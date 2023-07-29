/* Copyright 2023 Hewlett Packard Enterprise Development LP.
 * 
 * You can redistribute this program and/or modify it under the terms of
 * the GNU Lesser Public License version 2.1
 */
using BoltDB.Enums;
using System.IO;
using Utilities;

namespace BoltDB
{
    internal class PageHeader
    {
        public ulong ID { get; private set; }

        public PageFlags Flags { get; private set;}

        public ushort Count { get; private set; }

        public uint Overflow { get; private set; }

        //public uint Pointer { get; private set; }
        
        public PageHeader(byte[] page, ref int offset)
        {
            ID = LittleEndianReader.ReadUInt64(page, ref offset);
            Flags = (PageFlags)LittleEndianReader.ReadUInt16(page, ref offset);
            Count = LittleEndianReader.ReadUInt16(page, ref offset);
            Overflow = LittleEndianReader.ReadUInt32(page, ref offset);
            //Pointer = LittleEndianReader.ReadUInt32(stream);
        }

        public void Write(Stream stream)
        {
            LittleEndianWriter.WriteUInt64(stream, ID);
            LittleEndianWriter.WriteUInt16(stream, (ushort)Flags);
            LittleEndianWriter.WriteUInt16(stream, Count);
            LittleEndianWriter.WriteUInt32(stream, Overflow);
            //LittleEndianWriter.WriteUInt32(stream, Pointer);
        }
        
        public bool IsBranchPage
        {
            get
            {
                return (Flags & PageFlags.BranchPage) > 0;
            }
        }

        public bool IsLeafPage
        {
            get
            {
                return (Flags & PageFlags.LeafPage) > 0;
            }
        }

        public bool IsMetaPage
        {
            get
            {
                return (Flags & PageFlags.MetaPage) > 0;
            }
        }

        public bool IsFreeListPage
        {
            get
            {
                return (Flags & PageFlags.FreeListPage) > 0;
            }
        }
    }
}
