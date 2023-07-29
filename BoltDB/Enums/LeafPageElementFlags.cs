/* Copyright 2023 Hewlett Packard Enterprise Development LP.
 * 
 * You can redistribute this program and/or modify it under the terms of
 * the GNU Lesser Public License version 2.1
 */
using System;

namespace BoltDB
{
    [Flags]
    internal enum LeafPageElementFlags : uint
    {
        Bucket = 0x0001,
    }
}
