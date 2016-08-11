using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml.Serialization;
using Xamarin.Forms;
using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace FSDB.Core
{
    public class TableSetting
    {
        public string Type { get; set; }
        public string PK { get; set; }
    }

    public class FSConfig
    {
        public string DbName { get; set; }
        public int IdleTime { get; set; }
        public List<TableSetting> Settings { get; set; }
        public FSConfig()
        {
            Settings = new List<TableSetting>();
        }
    }
    public class FSData
    {
        public static FSManager Db { get; set; }
        public static FSConfig DbConfig { get; set; }
        public static void Init(FSConfig config)
        {
            DbConfig = config;
            var path = $"{Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)}/{config.DbName}";
            FSData.Db = new FSManager() { DocFolder = path };
        }
        public static void Init(string configFileName)
        {
            var ext = configFileName.Substring(configFileName.LastIndexOf('.'));
            var config = GetObject(ext);
            Init(config);
        }
        private static string GetDeviceName()
        {
            var name = string.Empty;
            switch (Device.OS)
            {
                case TargetPlatform.iOS:
                    name = "iOS";
                    break;
                case TargetPlatform.Android:
                    name = "Droid";
                    break;
                case TargetPlatform.Windows:
                    name = "UWP";
                    break;
            }
            return name;
        }
        private static FSConfig GetObject(string ext)
        {
            var typeInfo = Application.Current.GetType().GetTypeInfo();
            var fn = $"{typeInfo.Namespace}.{GetDeviceName()}.FSConfig{ext}";
            var assembly = typeInfo.Assembly;
            using (var stream = assembly.GetManifestResourceStream(fn))
            {
                using (var sr = new StreamReader(stream))
                {
                    if (ext.EndsWith("xml", StringComparison.CurrentCultureIgnoreCase))
                    {
                        var serializer = new XmlSerializer(typeof(FSConfig));
                        return (FSConfig)serializer.Deserialize(sr);
                    }
                    else {
                        var data = sr.ReadToEnd();
                        return JsonConvert.DeserializeObject<FSConfig>(data);
                    }
                }
            }
        }
        public static async void Stop()
        {
            await FSData.Db.ReleaseAll();
        }
    }
    public interface IFSTableModel
    {
        Guid CorrelationID { get; set; }
        DateTimeOffset TimeStamp { get; set; }
        bool MarkedForDelete { get; set; }
    }
    public abstract class FSTableModel : IFSTableModel, ICloneable, IDisposable, INotifyPropertyChanged
    {
        public Guid CorrelationID { get; set; }
        public DateTimeOffset TimeStamp { get; set; }
        public bool MarkedForDelete { get; set; }

        public event PropertyChangedEventHandler PropertyChanged;

        public object Clone()
        {
            var obj = Activator.CreateInstance(this.GetType());
            foreach (var prop in this.GetType().GetProperties())
                prop.SetValue(obj, prop.GetValue(this));
            return obj;

        }

        public virtual void Dispose() { }

        protected bool SetProperty<T>(
            ref T backingStore, T value,
            [CallerMemberName]string propertyName = "",
            Action onChanged = null)
        {
            if (EqualityComparer<T>.Default.Equals(backingStore, value))
                return false;

            backingStore = value;

            if (onChanged != null)
                onChanged();

            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));

            return true;
        }
        protected void OnPropertyChanged(string propName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propName));
        }
    }
    public class ArchiveModel : FSTableModel
    {
        public IFSTableModel HistoricalObject
        {
            get;
            set;
        }
    }

    public class FSTable : IDisposable
    {
        private int idleCounter;
        private TaskTimer timer;
        public int idleLimit;
        public string ModelKeyName { get; private set; }
        public FileStream Stream { get; set; }
        public Dictionary<Guid, IFSTableModel> Dict { get; set; }
        public Dictionary<object, Guid> ModelKeyDict { get; set; }

        public void Load<T>(List<T> collection) where T : IFSTableModel, new()
        {
            if (Dict == null)
                Dict = new Dictionary<Guid, IFSTableModel>();
            if (ModelKeyDict == null)
                ModelKeyDict = new Dictionary<object, Guid>();

            collection?.ForEach((obj) =>
            {
                AddToInstance(obj);
            });
            SetIdleCounter();
        }
        public void Load<T>(IFSTableModel obj) where T : IFSTableModel, new()
        {
            if (Dict == null)
                Dict = new Dictionary<Guid, IFSTableModel>();
            if (ModelKeyDict == null)
                ModelKeyDict = new Dictionary<object, Guid>();

            AddToInstance(obj);
            SetIdleCounter();
        }
        public bool IsEmpty
        {
            get
            {
                if (Dict == null || Dict.Count == 0)
                    return true;
                else
                    return false;
            }
        }
        public List<T> GetCollection<T>() where T : IFSTableModel, new()
        {
            if (IsEmpty)
            {
                return new List<T>();
            }
            else {
                SetIdleCounter();
                var temp = new List<T>();
                foreach (var key in Dict.Keys)
                    temp.Add((T)Dict[key]);
                return temp;
            }
        }

        public void Dispose()
        {
            timer.Cancel();
            Dict.Clear();
            ModelKeyDict.Clear();
        }

        public FSTable(int idleLimit, string modelKeyName)
        {
            this.ModelKeyName = modelKeyName;
            this.idleLimit = idleLimit;
            SetIdleCounter();

            timer = new TaskTimer((instance) =>
            {
                idleCounter--;
                if (idleCounter < 0 && Dict.Count > 0)
                {
                    var name = Dict.First().Value.GetType().Name;
                    Dict.Clear();
                    ModelKeyDict.Clear();

                }
                instance.ProceedWith();
            }, new TimeSpan(0, 1, 0));
        }

        private void SetIdleCounter()
        {
            idleCounter = this.idleLimit;
        }
        private void AddToInstance(IFSTableModel obj)
        {
            Dict.Add(obj.CorrelationID, obj);
            var prop = obj.GetType().GetProperty(ModelKeyName);
            var pk = prop.GetValue(obj, null);
            var defaultValue = DefaultForType(prop.PropertyType);
            var isDefault = pk.ToString().Equals(defaultValue.ToString());
            if (!isDefault && !ModelKeyDict.ContainsKey(pk))
            {
                ModelKeyDict.Add(pk, obj.CorrelationID);
            }
        }
        public object GetPrimaryValue(object obj)
        {
            var prop = obj.GetType().GetProperty(ModelKeyName);
            return prop.GetValue(obj, null);
        }
        public object DefaultForType(Type targetType)
        {
            return targetType.IsValueType ? Activator.CreateInstance(targetType) : null;
        }

    }

    internal delegate void TaskTimerCallback(TaskTimer instance);

    internal sealed class TaskTimer : CancellationTokenSource, IDisposable
    {
        private double dueTime;
        private TaskTimerCallback callback;

        internal TaskTimer(TaskTimerCallback callback, TimeSpan span)
        {
            this.callback = callback;
            this.dueTime = span.TotalMilliseconds;
            ProceedWith();
        }

        public void ProceedWith()
        {

            Task.Delay((int)dueTime, Token).ContinueWith((t, s) =>
            {
                var tuple = (Tuple<TaskTimerCallback>)s;
                tuple.Item1(this);
            }, Tuple.Create(callback), CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnRanToCompletion,
                TaskScheduler.Default);
        }

        public new void Dispose() { base.Cancel(); }
    }

    public class FSManager
    {
        private Dictionary<string, FSTable> dataDictionary;
        private Dictionary<Guid, IFSTableModel> archiveDictionary;
        private FileStream fsArchive;
        private SemaphoreSlim semaphore;
        public string DocFolder { get; set; } = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        public FSManager()
        {
            semaphore = new SemaphoreSlim(1);
            dataDictionary = new Dictionary<string, FSTable>();
        }

        public async Task ReleaseAll()
        {
            await Task.Run(() =>
            {
                foreach (var key in dataDictionary.Keys)
                {
                    dataDictionary[key].Stream.Close();
                    dataDictionary[key].Dispose();
                }
                dataDictionary.Clear();
            });
        }
        public async Task ReleaseTableMemory<T>() where T : IFSTableModel, new()
        {
            await Task.Run(async () =>
             {
                 await semaphore.WaitAsync();
                 try
                 {
                     var instance = GetDictionaryInstance<T>();
                     if (!instance.IsEmpty)
                     {
                         instance.Dict.Clear();
                         instance.ModelKeyDict.Clear();
                     }
                 }
                 catch (Exception ex)
                 {
                     LogError(ex);
                 }
                 finally
                 {
                     semaphore.Release();
                 }
             });
        }

        public async Task AddOrUpdate<T>(List<T> collection, bool useCorrelation = false) where T : IFSTableModel, new()
        {

            await Task.Run(async () =>
             {
                 await semaphore.WaitAsync();
                 try
                 {
                     var instance = GetDictionaryInstance<T>();
                     if (instance.IsEmpty)
                     {
                         var temp = await ReadFromFile<T>();
                         instance?.Load<T>(temp);
                     }

                     foreach (var obj in collection)
                     {
                         if (obj.CorrelationID == default(Guid))
                             obj.CorrelationID = Guid.NewGuid();

                         await Archive<T>(obj);
                         obj.TimeStamp = DateTimeOffset.Now;

                         if (useCorrelation)
                         {
                             if (instance.Dict.ContainsKey(obj.CorrelationID))
                             {
                                 instance.Dict[obj.CorrelationID] = obj;
                             }
                             else {
                                 instance.Load<T>(obj);
                             }
                         }
                         else {
                             var pk = instance.GetPrimaryValue(obj);
                             if (instance.ModelKeyDict.ContainsKey(pk))
                             {
                                 var correlationId = instance.ModelKeyDict[pk];
                                 instance.Dict[correlationId] = obj;
                             }
                             else {
                                 instance.Load<T>(obj);
                             }
                         }
                     }

                     await SaveToFile(instance.GetCollection<T>());
                     await ArchiveCommit();
                 }
                 catch (Exception ex)
                 {
                     LogError(ex);
                 }
                 finally
                 {
                     semaphore.Release();
                 }

             });
        }


        public async Task AddOrUpdate<T>(T obj, bool useCorrelation = false) where T : IFSTableModel, new()
        {
            await Task.Run(async () =>
            {
                var list = new List<T>(new T[] { obj });
                await AddOrUpdate<T>(list, useCorrelation);
            });

        }
        public async Task Delete<T>(Guid id, bool markForDelete = true) where T : IFSTableModel, new()
        {
            await Task.Run(async () =>
            {
                var instance = GetDictionaryInstance<T>();
                if (instance.IsEmpty)
                {
                    var temp = await ReadFromFile<T>();
                    instance?.Load<T>(temp);
                }
                if (instance.Dict.ContainsKey(id))
                {
                    if (markForDelete)
                    {
                        instance.Dict[id].MarkedForDelete = true;
                    }
                    else {
                        instance.Dict.Remove(id);
                    }
                    await SaveToFile(instance.GetCollection<T>());
                }

            });
        }
        public async Task Delete<T>(T obj) where T : IFSTableModel, new()
        {
            await Delete<T>(obj.CorrelationID);
        }
        public async Task<List<T>> GetAll<T>(bool includeDeleted = false) where T : IFSTableModel, new()
        {
            return await Task.Run(async () =>
            {
                await semaphore.WaitAsync();
                try
                {
                    var instance = GetDictionaryInstance<T>();
                    if (instance.IsEmpty)
                    {
                        var temp = await ReadFromFile<T>();
                        instance?.Load<T>(temp);
                    }
                    var query = instance.GetCollection<T>();
                    if (!includeDeleted)
                        query = query.Where<T>(x => x.MarkedForDelete == false).ToList<T>();

                    return query;
                }
                catch (Exception ex)
                {
                    LogError(ex);
                    return new List<T>();
                }
                finally
                {
                    semaphore.Release();
                }

            });
        }
        public async Task<List<T>> GetByQuery<T>(Func<T, bool> query, bool includeDeleted = false) where T : IFSTableModel, new()
        {
            return await Task.Run(async () =>
            {
                var list = await GetAll<T>(includeDeleted);
                return list.Where<T>(query).ToList<T>();
            });
        }
        private async Task<List<T>> ReadFromFile<T>() where T : IFSTableModel, new()
        {
            return await Task.Run(async () =>
            {
                var stream = dataDictionary[typeof(T).Name].Stream;
                var buffer = new byte[stream.Length];
                stream.Seek(0, 0);
                await stream.ReadAsync(buffer, 0, buffer.Length);
                var str = Encoding.UTF8.GetString(buffer);
                if (string.IsNullOrEmpty(str) || str == "null")
                    return new List<T>();
                else
                    return JsonConvert.DeserializeObject<List<T>>(str);
            });
        }
        private async Task SaveToFile<T>(List<T> collection) where T : IFSTableModel, new()
        {
            await Task.Run(async () =>
            {
                var stream = dataDictionary[typeof(T).Name].Stream;
                stream.Seek(0, 0);
                var str = JsonConvert.SerializeObject(collection);
                var buffer = Encoding.UTF8.GetBytes(str);
                if (stream.Length > buffer.Length)
                {
                    var originalBuffer = new byte[stream.Length];
                    buffer.CopyTo(originalBuffer, 0);
                    await stream.WriteAsync(originalBuffer, 0, originalBuffer.Length);
                }
                else {
                    await stream.WriteAsync(buffer, 0, buffer.Length);
                }
                await stream.FlushAsync();
            });
        }
        public async Task OpenTable<T>() where T : IFSTableModel, new()
        {
            await Task.Run(() =>
            {
                GetDictionaryInstance<T>();
            });
        }
        private FSTable GetDictionaryInstance<T>() where T : IFSTableModel
        {

            var name = typeof(T).Name;
            if (!dataDictionary.ContainsKey(name))
            {
                var setting = FSData.DbConfig.Settings.FirstOrDefault(x => x.Type == name);
                if (setting != null)
                {
                    if (!Directory.Exists(DocFolder))
                        Directory.CreateDirectory(DocFolder);

                    var pathToDatabase = Path.Combine(DocFolder, name + ".db");
                    var fs = new FileStream(pathToDatabase,
                        FileMode.OpenOrCreate,
                        FileAccess.ReadWrite,
                        FileShare.None);
                    dataDictionary.Add(name, new FSTable(FSData.DbConfig.IdleTime, setting.PK) { Stream = fs });
                }
            }
            return dataDictionary[name];


        }
        public async Task Rollback<T>(T obj) where T : IFSTableModel, new()
        {
            await Task.Run(() =>
            {
                if (archiveDictionary.ContainsKey(obj.CorrelationID))
                    obj = (T)archiveDictionary[obj.CorrelationID];
            });
        }
        private async Task ArchiveCommit()
        {
            await Task.Run(async () =>
            {
                if (fsArchive == null)
                {
                    var pathToDatabase = Path.Combine(DocFolder, "archive.db");
                    fsArchive = new FileStream(pathToDatabase,
                         FileMode.OpenOrCreate,
                         FileAccess.ReadWrite,
                     FileShare.None);
                }

                fsArchive.Seek(0, 0);
                var writeString = JsonConvert.SerializeObject(archiveDictionary);
                var writeBuffer = Encoding.UTF8.GetBytes(writeString);
                if (fsArchive.Length > writeBuffer.Length)
                {
                    var originalBuffer = new byte[fsArchive.Length];
                    writeBuffer.CopyTo(originalBuffer, 0);
                    await fsArchive.WriteAsync(originalBuffer, 0, originalBuffer.Length);
                }
                else {
                    await fsArchive.WriteAsync(writeBuffer, 0, writeBuffer.Length);
                }
                await fsArchive.FlushAsync();


            });
        }
        private async Task Archive<T>(T obj) where T : IFSTableModel, new()
        {
            await Task.Run(() =>
            {
                if (archiveDictionary == null)
                    archiveDictionary = new Dictionary<Guid, IFSTableModel>();

                if (archiveDictionary.ContainsKey(obj.CorrelationID))
                    archiveDictionary[obj.CorrelationID] = obj;
                else
                    archiveDictionary.Add(obj.CorrelationID, obj);

            });
        }
        private void LogError(Exception ex)
        {
            ex.WriteDebug(ex.Message);

        }
    }

    public static class FSExtensions
    {

        public static void WriteDebug(this object obj, string message)
        {
#if DEBUG
            Console.WriteLine($"*****************  DEBUG - {obj.GetType().Name}   *******************");
            Console.WriteLine($"*-*-*-* {message} *-*-*-*");
            Console.WriteLine("***********************************************************************");
#endif
        }

        public static string ToPropertyName<T, P>(this Expression<Func<T, P>> exp) where T : IFSTableModel, new()
        {
            var expression = (MemberExpression)exp.Body;
            return expression.Member.Name;
        }
    }

    //public static class Extensions
    //{
    //    public static IList<T> Clone<T>(this List<T> listToClone) where T : IFSTableModel, ICloneable
    //    {
    //        var list = new List<T>();
    //        listToClone.ForEach((x) => list.Add((T)x.Clone()));
    //        return list;
    //    }
    //}
}



