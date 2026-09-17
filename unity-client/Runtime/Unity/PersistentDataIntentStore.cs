using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using UnityEngine;

namespace PaxosArena.Client.UnityAdapters
{
    /// <summary>
    /// <see cref="IIntentStore"/> in one JSON file under
    /// Application.persistentDataPath/paxos-arena/client-state.json (section 11.3).
    ///
    /// Save writes client-state.json.tmp, flushes it to disk, then replaces the
    /// file: File.Replace with a .bak backup where the platform supports it,
    /// otherwise copy to .bak, delete and move. Load reads the file; when it is
    /// missing or does not parse it tries the .tmp (a crash after the .tmp was
    /// flushed and before the replace leaves the newest state there; a torn
    /// .tmp does not parse), then the .bak. When files exist but none parses,
    /// Load moves them aside as client-state.json.corrupt-N (and .tmp and .bak
    /// likewise) before it returns a fresh state, so the new credentials the
    /// client then draws never overwrite a device secret someone may recover.
    ///
    /// Only one store may be open on a directory in a process: a second one
    /// throws <see cref="InvalidOperationException"/> until the first is
    /// disposed, because two clients on one file overwrite each other's
    /// intents and assign sequence numbers twice.
    ///
    /// Every Save is synchronous on the calling thread, the main thread in
    /// Unity: it serialises the whole state (up to about 25 KB with sixteen
    /// deal audits) and waits for an fsync. A move costs two saves, one when
    /// the intent is created and one when its answer arrives.
    ///
    /// The file holds the device secret. On iOS the directory is excluded
    /// from iCloud and device backups. On Android, exclude
    /// files/paxos-arena/ in the application's backup rules, or a restore onto
    /// another device clones the player (see the package README). A game that
    /// keeps the secret in the platform's keystore implements
    /// <see cref="IIntentStore"/> itself.
    /// </summary>
    public sealed class PersistentDataIntentStore : IIntentStore, IDisposable
    {
        public const string DirectoryName = "paxos-arena";
        public const string FileName = "client-state.json";

        /// <summary>How many corrupt-N names Load tries before it overwrites the last.</summary>
        const int MaxCorruptCopies = 100;

        static readonly HashSet<string> OpenPaths = new HashSet<string>(StringComparer.Ordinal);

        readonly IJson json;
        readonly string directory;
        readonly string path;
        readonly string tempPath;
        readonly string backupPath;
        bool disposed;
        bool directoryReady;

        /// <summary>Stores under Application.persistentDataPath; construct it on the main thread, for example in Awake.</summary>
        public PersistentDataIntentStore(IJson json)
            : this(json, Path.Combine(Application.persistentDataPath, DirectoryName))
        {
        }

        /// <summary>Stores in the given directory. Throws InvalidOperationException while another open store uses it.</summary>
        public PersistentDataIntentStore(IJson json, string directory)
        {
            if (json == null)
            {
                throw new ArgumentNullException(nameof(json));
            }
            if (string.IsNullOrEmpty(directory))
            {
                throw new ArgumentException("a directory is required", nameof(directory));
            }
            this.json = json;
            this.directory = Path.GetFullPath(directory);
            path = Path.Combine(this.directory, FileName);
            tempPath = path + ".tmp";
            backupPath = path + ".bak";
            lock (OpenPaths)
            {
                if (!OpenPaths.Add(path))
                {
                    throw new InvalidOperationException(
                        "another PersistentDataIntentStore is open on " + path +
                        "; use one ArenaClient per store, and dispose the store before opening it again");
                }
            }
        }

        /// <summary>The state file's full path.</summary>
        public string FilePath
        {
            get { return path; }
        }

        public ClientState Load()
        {
            ThrowIfDisposed();
            ClientState state = TryRead(path) ?? TryRead(tempPath) ?? TryRead(backupPath);
            if (state != null)
            {
                return state;
            }
            MoveAside(path);
            MoveAside(tempPath);
            MoveAside(backupPath);
            return new ClientState();
        }

        public void Save(ClientState state)
        {
            ThrowIfDisposed();
            if (state == null)
            {
                throw new ArgumentNullException(nameof(state));
            }
            byte[] bytes = Encoding.UTF8.GetBytes(json.ToJson(state));
            PrepareDirectory();
            using (FileStream stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                stream.Write(bytes, 0, bytes.Length);
                stream.Flush(true);
            }
            if (!File.Exists(path))
            {
                File.Move(tempPath, path);
                return;
            }
            try
            {
                File.Replace(tempPath, path, backupPath);
            }
            catch (PlatformNotSupportedException)
            {
                ReplaceByMove();
            }
            catch (NotSupportedException)
            {
                ReplaceByMove();
            }
            catch (IOException)
            {
                // Some file systems refuse the atomic replace; the temporary
                // file is complete, so fall back to copy, delete and move.
                if (!File.Exists(tempPath))
                {
                    throw;
                }
                ReplaceByMove();
            }
        }

        /// <summary>Releases the directory for another store; the files stay.</summary>
        public void Dispose()
        {
            if (disposed)
            {
                return;
            }
            disposed = true;
            lock (OpenPaths)
            {
                OpenPaths.Remove(path);
            }
        }

        void PrepareDirectory()
        {
            if (directoryReady)
            {
                return;
            }
            Directory.CreateDirectory(directory);
#if UNITY_IOS && !UNITY_EDITOR
            // The device secret must not reach iCloud or device backups
            // (contract section 9.1: for the life of the installation).
            UnityEngine.iOS.Device.SetNoBackupFlag(directory);
#endif
            directoryReady = true;
        }

        void ReplaceByMove()
        {
            File.Copy(path, backupPath, true);
            File.Delete(path);
            File.Move(tempPath, path);
        }

        static void MoveAside(string file)
        {
            try
            {
                if (!File.Exists(file))
                {
                    return;
                }
                string target = file + ".corrupt-" + MaxCorruptCopies;
                for (int n = 1; n < MaxCorruptCopies; n++)
                {
                    string candidate = file + ".corrupt-" + n;
                    if (!File.Exists(candidate))
                    {
                        target = candidate;
                        break;
                    }
                }
                File.Copy(file, target, true);
                File.Delete(file);
            }
            catch (Exception)
            {
                // Best effort: a file that cannot be moved is overwritten by
                // the next save, as before.
            }
        }

        ClientState TryRead(string file)
        {
            try
            {
                if (!File.Exists(file))
                {
                    return null;
                }
                string text = File.ReadAllText(file, Encoding.UTF8);
                if (string.IsNullOrEmpty(text) || text.Trim().Length == 0)
                {
                    return null;
                }
                ClientState state = json.FromJson<ClientState>(text);
                return state != null && state.version >= 1 ? state : null;
            }
            catch (Exception)
            {
                return null;
            }
        }

        void ThrowIfDisposed()
        {
            if (disposed)
            {
                throw new ObjectDisposedException(nameof(PersistentDataIntentStore));
            }
        }
    }
}
