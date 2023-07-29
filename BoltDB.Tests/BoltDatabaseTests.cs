/* Copyright 2023 Hewlett Packard Enterprise Development LP.
 * 
 * You can redistribute this program and/or modify it under the terms of
 * the GNU Lesser Public License version 2.1
 */
using BoltDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Text;

namespace Bolt.Net.Tests
{
    [TestClass]
    public class BoltDatabaseTests
    {
        [TestMethod]
        [DeploymentItem(@"Databases\NoRoot.db", @"Databases\NoRoot.db")]
        public void TestReadingDatabaseWithEmptyRootBucket()
        {
            BoltDatabase boltDB = new BoltDatabase(@"Databases\NoRoot.db");
            Bucket rootBucket = boltDB.GetRootBucket();
            List<KeyValuePair<string, object>> rootElements = rootBucket.GetElements();
            Assert.AreEqual(0, rootElements.Count);
        }

        [TestMethod]
        [DeploymentItem(@"Databases\RootBucketWith15Pairs.db", @"Databases\RootBucketWith15Pairs.db")]
        public void TestReadingDatabaseWithRootBucketContaining15Pairs()
        {
            BoltDatabase boltDB = new BoltDatabase(@"Databases\RootBucketWith15Pairs.db");
            Bucket rootBucket = boltDB.GetRootBucket();
            List<KeyValuePair<string, object>> rootElements = rootBucket.GetElements();
            Assert.AreEqual(1, rootElements.Count);
            Assert.AreEqual("Root Bucket", rootElements[0].Key);
            Assert.IsInstanceOfType(rootElements[0].Value, typeof(Bucket));
            Bucket userRootBucket = rootElements[0].Value as Bucket;
            List<KeyValuePair<string, object>> elements = userRootBucket.GetElements();
            Assert.AreEqual(15, elements.Count);

            for (int index = 0; index < elements.Count; index++)
            {
                Assert.IsInstanceOfType(elements[index].Value, typeof(byte[]));
                Assert.AreEqual((index + 1).ToString(), Encoding.UTF8.GetString(elements[index].Value as byte[]));
            }
        }

        [TestMethod]
        [DeploymentItem(@"Databases\RootBucketWith152Pairs.db", @"Databases\RootBucketWith152Pairs.db")]
        public void TestReadingDatabaseWithRootBucketContaining152Pairs()
        {
            BoltDatabase boltDB = new BoltDatabase(@"Databases\RootBucketWith152Pairs.db");
            Bucket rootBucket = boltDB.GetRootBucket();
            List<KeyValuePair<string, object>> rootElements = rootBucket.GetElements();
            Assert.AreEqual(1, rootElements.Count);
            Bucket userRootBucket = rootElements[0].Value as Bucket;
            Assert.IsNotNull(userRootBucket);
            Assert.AreEqual("Root Bucket", rootElements[0].Key);
            List<KeyValuePair<string, object>>  elements = userRootBucket.GetElements();
            Assert.AreEqual(152, elements.Count);

            for (int index = 0; index < elements.Count; index++)
            {
                Assert.IsInstanceOfType(elements[index].Value, typeof(byte[]));
                Assert.AreEqual((index + 1).ToString("000"), Encoding.UTF8.GetString(elements[index].Value as byte[]));
            }
        }
    }
}