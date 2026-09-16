using System;
using System.IO;
using System.Text;
using UnityEngine;

namespace PaxosArena.Client.Unity
{
    /// <summary>
    /// <see cref="IIntentStore"/> in one JSON file under
    /// Application.persistentDataPath/paxos-arena/client-state.json (section 11.3).
    ///
    /// Save writes client-state.json.tmp, flushes it to disk, then replaces the
    /// file: File.Replace with a .bak backup where the platform supports it,
    /// otherwise copy to .bak, delete and move. Load reads the file; if it is
    /// missing it tries the .tmp (a crash between the delete and the move of
    /// the fallback leaves the complete new state there), then the .bak; it
    /// returns a fresh state when nothing parses.
    ///
    /// The file holds the device secret. A game that keeps the secret in the
    /// platform's keystore implements <see cref="IIntentStore"/> itself.
    /// </summary>
    public sealed class PersistentDataIntentStore : IIntentStore
    {
        public const string DirectoryName = "paxos-arena";
        public const string FileName = "client-state.json";

        readonly IJson json;
        readonly string directory;
        readonly string path;
        readonly string tempPath;
        readonly string backupPath;

        /// <summary>Stores under Application.persistentDataPath; construct it on the main thread, for example in Awake.</summary>
        public PersistentDataIntentStore(IJson json)
            : this(json, Path.Combine(Application.persistentDataPath, DirectoryName))
        {
        }

        /// <summary>Stores in the given directory.</summary>
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
            this.directory = directory;
            path = Path.Combine(directory, FileName);
            tempPath = path + ".tmp";
            backupPath = path + ".bak";
        }

        /// <summary>The state file's full path.</summary>
        public string FilePath
        {
            get { return path; }
        }

        public ClientState Load()
        {
            ClientState state = TryRead(path);
            if (state == null && !File.Exists(path))
            {
                state = TryRead(tempPath);
            }
            if (state == null)
            {
                state = TryRead(backupPath);
            }
            return state ?? new ClientState();
        }

        public void Save(ClientState state)
        {
            if (state == null)
            {
                throw new ArgumentNullException(nameof(state));
            }
            byte[] bytes = Encoding.UTF8.GetBytes(json.ToJson(state));
            Directory.CreateDirectory(directory);
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
                // file is complete, so fall back to delete and move.
                if (!File.Exists(tempPath))
                {
                    throw;
                }
                ReplaceByMove();
            }
        }

        void ReplaceByMove()
        {
            File.Copy(path, backupPath, true);
            File.Delete(path);
            File.Move(tempPath, path);
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
    }
}
