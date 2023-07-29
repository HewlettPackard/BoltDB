/* Copyright 2023 Hewlett Packard Enterprise Development LP.
 * 
 * You can redistribute this program and/or modify it under the terms of
 * the GNU Lesser Public License version 2.1
 */
using System;

namespace BoltDB.Enums
{
    [Flags]
    internal enum PageFlags : ushort
    {
        BranchPage = 0x01,
        LeafPage = 0x02,
        MetaPage = 0x04,
        FreeListPage = 0x10,
    }
}
