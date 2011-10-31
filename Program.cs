using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Net;

namespace SyncToContainer
{
    class Program
    {
        static Semaphore semaphore = new Semaphore(32, 32);
        static int count = 0;

        static string GetMd5(string path)
        {
            using (var stream = File.OpenRead(path)) return Convert.ToBase64String(new MD5CryptoServiceProvider().ComputeHash(stream));
        }

        static void UploadWithMd5(CloudBlobContainer container, string name, string path)
        {
            var blob = container.GetBlobReference(name);
            blob.Properties.ContentMD5 = GetMd5(path);
            semaphore.WaitOne();
            var stream = File.OpenRead(path);
            Interlocked.Increment(ref count);
            blob.BeginUploadFromStream(stream, (ar) => {
                blob.EndUploadFromStream(ar);
                stream.Close();
                semaphore.Release();
                Interlocked.Decrement(ref count);
            }, null);
        }

        static void Main(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = 32;
            var container = new CloudStorageAccount(new StorageCredentialsAccountAndKey(args[1], args[2]), true).CreateCloudBlobClient().GetContainerReference(args[3]);
            container.CreateIfNotExist();

            var cloudHashes = new Dictionary<string, string>();
            foreach (CloudBlob blob in container.ListBlobs(new BlobRequestOptions { UseFlatBlobListing = true }))
            {
                cloudHashes[blob.Uri.ToString().Substring(container.Uri.ToString().Length + 1)] = blob.Properties.ContentMD5;
            }

            var localHashes = new Dictionary<string, string>();
            foreach (var file in Directory.EnumerateFiles(args[0], "*", SearchOption.AllDirectories))
            {
                localHashes[file.Substring(args[0].Length + 1).Replace('\\', '/')] = GetMd5(file);
            }

            foreach (var name in cloudHashes.Keys.Where(n => localHashes.ContainsKey(n) && (localHashes[n] != cloudHashes[n])))
            {
                Console.WriteLine("Uploading {0}", name);
                UploadWithMd5(container, name, Path.Combine(args[0], name));
            }
            foreach (var name in localHashes.Keys.Where(n => !cloudHashes.ContainsKey(n)))
            {
                Console.WriteLine("Uploading {0}", name);
                UploadWithMd5(container, name, Path.Combine(args[0], name));
            }
            foreach (var name in cloudHashes.Keys.Where(n => !localHashes.ContainsKey(n)))
            {
                Console.WriteLine("Deleting {0}", name);
                container.GetBlobReference(name).Delete();
            }

            while (count > 0) Thread.Sleep(TimeSpan.FromSeconds(1));
        }
    }
}
