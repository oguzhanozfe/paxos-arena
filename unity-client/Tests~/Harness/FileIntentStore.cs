using System;
using System.IO;
using System.Text;

namespace PaxosArena.Client.Harness
{
    /// <summary>
    /// IIntentStore in one file, used only by the harness: write a temporary
    /// file, flush it, rename it over the state file.
    /// </summary>
    public sealed class FileIntentStore : IIntentStore
    {
        readonly IJson json;
        readonly string path;

        public FileIntentStore(IJson json, string directory)
        {
            this.json = json;
            Directory.CreateDirectory(directory);
            path = Path.Combine(directory, "client-state.json");
        }

        public string FilePath
        {
            get { return path; }
        }

        public int Saves { get; private set; }

        public ClientState Load()
        {
            if (!File.Exists(path))
            {
                return new ClientState();
            }
            try
            {
                return json.FromJson<ClientState>(File.ReadAllText(path, Encoding.UTF8)) ?? new ClientState();
            }
            catch (Exception)
            {
                return new ClientState();
            }
        }

        public void Save(ClientState state)
        {
            string temp = path + ".tmp";
            byte[] bytes = Encoding.UTF8.GetBytes(json.ToJson(state));
            using (FileStream stream = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                stream.Write(bytes, 0, bytes.Length);
                stream.Flush(true);
            }
            File.Move(temp, path, true);
            Saves++;
        }
    }
}
