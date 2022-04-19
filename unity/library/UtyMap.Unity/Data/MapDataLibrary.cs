using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using UnityEngine;
using UtyDepend;
using UtyMap.Unity.Infrastructure.Diagnostic;
using UtyMap.Unity.Infrastructure.IO;
using UtyMap.Unity.Infrastructure.Primitives;
using UtyMap.Unity.Utils;
using UtyRx;

namespace UtyMap.Unity.Data
{
    /// <summary> Encapsulates low level API of underlying map data processing library. </summary>
    /// <remarks>
    ///     This interface provides the way to override default behavior of how map data is
    ///     added and received from native library. For example, il2cpp script backend requires 
    ///     to use different PInvoke mechanisms than mono.
    ///     Use <see cref="IMapDataStore"/> for any app specific logic.
    /// </remarks>
    public interface IMapDataLibrary : IDisposable
    {
        /// <summary> Configure utymap. Should be called before any core API usage. </summary>
        /// <param name="indexPath"> Path to index data. </param>
        void Configure(string indexPath);

        /// <summary> Enable mesh caching mechanism for speed up tile loading. </summary>
        void EnableCache();

        /// <summary> Disable mesh caching mechanism. </summary>
        void DisableCache();

        /// <summary> Checks whether there is data for given quadkey. </summary>
        /// <returns> True if there is data for given quadkey. </returns>
        bool Exists(QuadKey quadKey);

        /// <summary> Gets content of the tile notifying passed observers. </summary>
        /// <param name="tile"> Tile to load. </param>
        /// <param name="observers"> Observers to notify. </param>
        UtyRx.IObservable<int> Get(Tile tile, IList<UtyRx.IObserver<MapData>> observers);

        /// <summary> Gets elements matching given query </summary>
        /// <param name="query"> Search query. </param>
        /// <param name="observers"> Observers to notify. </param>
        UtyRx.IObservable<int> Get(MapQuery query, IList<UtyRx.IObserver<Element>> observers);

        /// <summary> Registers in-memory data storage with given key. </summary>
        /// <param name="storageKey"> Storage key.</param>
        void Register(string storageKey);

        /// <summary> Registers persistent data storage with given key. </summary>
        /// <param name="storageKey"> Storage key.</param>
        /// <param name="indexPath"> Persistent index path. </param>
        void Register(string storageKey, string indexPath);

        /// <summary>
        ///     Adds map data to data storage only to specific level of details.
        ///     Supported formats: shapefile, osm xml, geo json, osm pbf.
        /// </summary>
        /// <param name="storageKey"> Map data storage key. </param>
        /// <param name="path"> Path to file. </param>
        /// <param name="stylesheet"> Stylesheet path. </param>
        /// <param name="levelOfDetails"> Level of details. </param>
        /// <param name="cancellationToken"> Cancellation token. </param>
        UtyRx.IObservable<int> AddTo(string storageKey, string path, Stylesheet stylesheet, Range<int> levelOfDetails, CancellationToken cancellationToken);

        /// <summary>
        ///     Adds map data to data storage only to specific quadkey.
        ///     Supported formats: shapefile, osm xml, geo json, osm pbf.
        /// </summary>
        /// <param name="storageKey"> Map data storage key. </param>
        /// <param name="path"> Path to file. </param>
        /// <param name="stylesheet"> Stylesheet path. </param>
        /// <param name="quadKey"> QuadKey. </param>
        /// <param name="cancellationToken"> Cancellation token. </param>
        UtyRx.IObservable<int> AddTo(string storageKey, string path, Stylesheet stylesheet, QuadKey quadKey, CancellationToken cancellationToken);

        /// <summary>
        ///     Adds element to data storage to specific level of details.
        /// </summary>
        /// <param name="storageKey"> Map data storage key. </param>
        /// <param name="element"> Element to add. </param>
        /// <param name="stylesheet"> Stylesheet </param>
        /// <param name="levelOfDetails"> Level of detail range. </param>
        /// <param name="cancellationToken"> Cancellation token. </param>
        UtyRx.IObservable<int> AddTo(string storageKey, Element element, Stylesheet stylesheet, Range<int> levelOfDetails, CancellationToken cancellationToken);

        /// <summary>
        ///      Gets elevation for specific coordinate and quadKey.
        /// </summary>
        /// <param name="elevationDataType"></param>
        /// <param name="quadKey"></param>
        /// <param name="coordinate"></param>
        double GetElevation(ElevationDataType elevationDataType, QuadKey quadKey, GeoCoordinate coordinate);
    }

    /// <summary> Default implementation. </summary>
    internal class MapDataLibrary : IMapDataLibrary
    {
        private const string TraceCategory = "library";
        private readonly object __lockObj = new object();
        private readonly IPathResolver _pathResolver;
        private readonly ITrace _trace;
        private volatile bool _isConfigured;

        private HashSet<string> _stylePaths = new HashSet<string>();

        [Dependency]
        public MapDataLibrary(IPathResolver pathResolver, ITrace trace)
        {
            _pathResolver = pathResolver;
            _trace = trace;
        }

        /// <inheritdoc />
        public UtyRx.IObservable<int> Get(Tile tile, IList<UtyRx.IObserver<MapData>> observers)
        {
            var tileHandler = new TileHandler(tile, observers);
            return Get(tile, tile.GetHashCode(), tileHandler.OnMeshBuiltHandler, tileHandler.OnElementLoadedHandler, OnErrorHandler);
        }

        /// <inheritdoc />
        public UtyRx.IObservable<int> Get(MapQuery query, IList<UtyRx.IObserver<Element>> observers)
        {
            var queryHandler = new QueryHandler(observers);
            return Get(query, 0, queryHandler.OnElementLoadedHandler, OnErrorHandler);
        }

        /// <inheritdoc />
        public void Configure(string indexPath)
        {
            lock (__lockObj)
            {
                indexPath = _pathResolver.Resolve(indexPath);

                _trace.Debug(TraceCategory, "Configure with {0}", indexPath);
                // NOTE this directories should be created in advance (and some others..)
                if (!Directory.Exists(indexPath))
                    throw new DirectoryNotFoundException(String.Format("Cannot find {0}", indexPath));

                if (_isConfigured) return;

                // create directory for downloaded raw map data.
                CreateDirectory(Path.Combine(indexPath, "import"));

                connect(indexPath, OnErrorHandler);
                _isConfigured = true;
            }
        }

        /// <inheritdoc />
        public void EnableCache()
        {
            enableMeshCache(1);
        }

        /// <inheritdoc />
        public void DisableCache()
        {
            enableMeshCache(0);
        }

        /// <inheritdoc />
        public bool Exists(QuadKey quadKey)
        {
            return hasData(quadKey.TileX, quadKey.TileY, quadKey.LevelOfDetail);
        }

        /// <inheritdoc />
        public void Register(string storageKey)
        {
            registerInMemoryStore(storageKey);
        }

        /// <inheritdoc />
        public void Register(string storageKey, string indexPath)
        {
            registerPersistentStore(storageKey, _pathResolver.Resolve(indexPath), CreateDirectory);
        }

        /// <inheritdoc />
        public UtyRx.IObservable<int> AddTo(string storageKey, string path, Stylesheet stylesheet, Range<int> levelOfDetails, CancellationToken cancellationToken)
        {
            var dataPath = _pathResolver.Resolve(path);
            var stylePath = RegisterStylesheet(stylesheet);
            _trace.Debug(TraceCategory, "Add data from {0} to {1} storage", dataPath, storageKey);
            lock (__lockObj)
            {
                WithCancelToken(cancellationToken, (cancelTokenHandle) => addDataInRange(
                    storageKey, stylePath, dataPath, levelOfDetails.Minimum,
                    levelOfDetails.Maximum, OnErrorHandler, cancelTokenHandle.AddrOfPinnedObject()));
            }
            return Observable.Return<int>(100);
        }

        /// <inheritdoc />
        public UtyRx.IObservable<int> AddTo(string storageKey, string path, Stylesheet stylesheet, QuadKey quadKey, CancellationToken cancellationToken)
        {
            var dataPath = _pathResolver.Resolve(path);
            var stylePath = RegisterStylesheet(stylesheet);
            _trace.Debug(TraceCategory, "Add data from {0} to {1} storage", dataPath, storageKey);
            lock (__lockObj)
            {
                WithCancelToken(cancellationToken, (cancelTokenHandle) => addDataInQuadKey(
                    storageKey, stylePath, dataPath, quadKey.TileX, quadKey.TileY,
                    quadKey.LevelOfDetail, OnErrorHandler, cancelTokenHandle.AddrOfPinnedObject()));
            }
            return Observable.Return<int>(100);
        }

        /// <inheritdoc />
        public UtyRx.IObservable<int> AddTo(string storageKey, Element element, Stylesheet stylesheet, Range<int> levelOfDetails, CancellationToken cancellationToken)
        {
            _trace.Debug(TraceCategory, "Add element to {0} storage", storageKey);
            double[] coordinates = new double[element.Geometry.Length * 2];
            for (int i = 0; i < element.Geometry.Length; ++i)
            {
                coordinates[i * 2] = element.Geometry[i].Latitude;
                coordinates[i * 2 + 1] = element.Geometry[i].Longitude;
            }

            string[] tags = new string[element.Tags.Count * 2];
            var tagKeys = element.Tags.Keys.ToArray();
            for (int i = 0; i < tagKeys.Length; ++i)
            {
                tags[i * 2] = tagKeys[i];
                tags[i * 2 + 1] = element.Tags[tagKeys[i]];
            }

            var stylePath = RegisterStylesheet(stylesheet);

            lock (__lockObj)
            {
                WithCancelToken(cancellationToken, (cancelTokenHandle) => addDataInElement(
                    storageKey, stylePath, element.Id, coordinates, coordinates.Length, tags, tags.Length,
                    levelOfDetails.Minimum, levelOfDetails.Maximum, OnErrorHandler, cancelTokenHandle.AddrOfPinnedObject()));
            }
            return Observable.Return<int>(100);
        }

        /// <inheritdoc />
        public double GetElevation(ElevationDataType elevationDataType, QuadKey quadKey, GeoCoordinate coordinate)
        {
            return getElevationByQuadKey(quadKey.TileX, quadKey.TileY, quadKey.LevelOfDetail,
                (int)elevationDataType, coordinate.Latitude, coordinate.Longitude);
        }

        /// <inheritdoc />
        public void Dispose()
        {
        }

        #region Private members

        private UtyRx.IObservable<int> Get(Tile tile, int tag, OnMeshBuilt meshBuiltHandler, OnElementLoaded elementLoadedHandler, OnError errorHandler)
        {
            _trace.Debug(TraceCategory, "Get tile {0}", tile.ToString());
            var stylePath = RegisterStylesheet(tile.Stylesheet);
            var quadKey = tile.QuadKey;
            WithCancelToken(tile.CancelationToken, (cancelTokenHandle) => getDataByQuadKey(
                tag, stylePath, quadKey.TileX, quadKey.TileY, quadKey.LevelOfDetail,
                (int)tile.ElevationType, meshBuiltHandler, elementLoadedHandler, errorHandler,
                cancelTokenHandle.AddrOfPinnedObject())
            );
            return Observable.Return(100);
        }

        private UtyRx.IObservable<int> Get(MapQuery query, int tag, OnElementLoaded elementLoadedHandler, OnError errorHandler)
        {
            _trace.Debug(TraceCategory, "Search elements");
            WithCancelToken(new CancellationToken(), (cancelTokenHandle) => getDataByText(
                tag, query.NotTerms, query.AndTerms, query.OrTerms,
                query.BoundingBox.MinPoint.Latitude, query.BoundingBox.MinPoint.Longitude,
                query.BoundingBox.MaxPoint.Latitude, query.BoundingBox.MaxPoint.Longitude,
                query.LodRange.Minimum, query.LodRange.Maximum, elementLoadedHandler, errorHandler,
                cancelTokenHandle.AddrOfPinnedObject())
            );
            return Observable.Return(100);
        }

        private void WithCancelToken(CancellationToken token, Action<GCHandle> action)
        {
            var cancelTokenHandle = GCHandle.Alloc(token, GCHandleType.Pinned);
            try
            {
                action(cancelTokenHandle);
            }
            catch (Exception ex)
            {
                _trace.Error(TraceCategory, ex, "Cannot execute.");
            }
            finally
            {
                cancelTokenHandle.Free();
            }
        }

        private static void CreateDirectory(string directory)
        {
            Directory.CreateDirectory(directory);
        }

        private string RegisterStylesheet(Stylesheet stylesheet)
        {
            var stylePath = _pathResolver.Resolve(stylesheet.Path);

            if (_stylePaths.Contains(stylePath))
                return stylePath;

            _stylePaths.Add(stylePath);
            registerStylesheet(stylePath, CreateDirectory);

            return stylePath;
        }

        #endregion

        #region Delegates

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void OnNewDirectory([In] string directory);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void OnMeshBuilt(int tag, [In] string name,
            [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)] [In] double[] vertices, [In] int vertexCount,
            [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 5)] [In] int[] triangles, [In] int triangleCount,
            [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 7)] [In] int[] colors, [In] int colorCount,
            [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 9)] [In] double[] uvs, [In] int uvCount,
            [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 11)] [In] int[] uvMap, [In] int uvMapCount);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void OnElementLoaded(int tag, [In] long id,
            [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 3)] [In] string[] tags, [In] int tagCount,
            [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 5)] [In] double[] vertices, [In] int vertexCount,
            [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 7)] [In] string[] styles, [In] int styleCount);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void OnError([In] string message);

        #endregion

        #region Callbacks

        private class TileHandler
        {
            private readonly Tile _tile;
            private readonly IList<UtyRx.IObserver<MapData>> _observers;

            public TileHandler(Tile tile, IList<UtyRx.IObserver<MapData>> observers)
            {
                _tile = tile;
                _observers = observers;
            }

            public void OnMeshBuiltHandler(int tag, string name, double[] vertices, int vertexCount,
                int[] triangles, int triangleCount, int[] colors, int colorCount,
                double[] uvs, int uvCount, int[] uvMap, int uvMapCount)
            {
                Mesh mesh = GetMesh(_tile, name, vertices, triangles, colors);
                NotifyObservers(new MapData(_tile, new Union<Element, Mesh>(mesh)));
            }

            public void OnElementLoadedHandler(int tag, long id, string[] tags, int tagCount,
                double[] vertices, int vertexCount, string[] styles, int styleCount)
            {
                Element element = GetElement(id, tags, vertices, styles);
                NotifyObservers(new MapData(_tile, new Union<Element, Mesh>(element)));
            }

            private void NotifyObservers(MapData mapData)
            {
                foreach (var observer in _observers)
                    observer.OnNext(mapData);
            }
        }

        private class QueryHandler
        {
            private readonly IList<UtyRx.IObserver<Element>> _observers;

            public QueryHandler(IList<UtyRx.IObserver<Element>> observers)
            {
                _observers = observers;
            }

            public void OnElementLoadedHandler(int tag, long id, string[] tags, int tagCount,
                double[] vertices, int vertexCount, string[] styles, int styleCount)
            {
                Element element = GetElement(id, tags, vertices, styles);
                foreach (var observer in _observers)
                    observer.OnNext(element);
            }
        }

        private static void OnErrorHandler(string message)
        {
            throw new InvalidOperationException(message);
        }

        #endregion

        #region Helpers

        private static Dictionary<string, string> ReadDict(string[] data)
        {
            var length = data == null ? 0 : data.Length;
            var map = new Dictionary<string, string>(length / 2);
            for (int i = 0; i < length; i += 2)
                map.Add(data[i], data[i + 1]);
            return map;
        }

        private static Mesh GetMesh(Tile tile, string name, double[] vertices, int[] triangles,  int[] colors)
        {
            var worldPoints = new Vector3[vertices.Length / 3];
            for (int i = 0; i < vertices.Length; i += 3)
                worldPoints[i / 3] = tile.Projection
                    .Project(new GeoCoordinate(vertices[i + 1], vertices[i]), vertices[i + 2]);

            var colorCount = colors.Length;
            var unityColors = new Color[colorCount];
            for (int i = 0; i < colorCount; ++i)
                unityColors[i] = ColorUtils.FromInt(colors[i]);

            var triangleCount = triangles.Length;
            var unityUvs = new Vector2[triangleCount];
            var unityUvs2 = new Vector2[triangleCount];
            var unityUvs3 = new Vector2[triangleCount];

            return new Mesh(name, 0, worldPoints, triangles, unityColors, unityUvs, unityUvs2, unityUvs3);
        }

        private static Element GetElement(long id, string[] tags, double[] vertices, string[] styles)
        {
            var vertexCount = vertices.Length;
            var geometry = new GeoCoordinate[vertexCount / 3];
            var heights = new double[vertexCount / 3];
            for (int i = 0; i < vertexCount; i += 3)
            {
                geometry[i / 3] = new GeoCoordinate(vertices[i + 1], vertices[i]);
                heights[i / 3] = vertices[i + 2];
            }

            return new Element(id, geometry, heights, ReadDict(tags), ReadDict(styles));
        }

        #endregion

        #region PInvoke import

        #region Lifecycle API

        [DllImport("UtyMap.Shared")]
        private static extern void connect(string stringPath, OnError errorHandler);

        [DllImport("UtyMap.Shared")]
        private static extern void disconnect();

        #endregion

        #region Configuration API

        [DllImport("UtyMap.Shared")]
        private static extern void enableMeshCache(int enabled);

        [DllImport("UtyMap.Shared")]
        private static extern void registerStylesheet(string path, OnNewDirectory directoryHandler);

        [DllImport("UtyMap.Shared")]
        private static extern void registerInMemoryStore(string key);

        [DllImport("UtyMap.Shared")]
        private static extern void registerPersistentStore(string key, string path, OnNewDirectory directoryHandler);

        #endregion

        #region Storage API

        [DllImport("UtyMap.Shared")]
        private static extern void addDataInRange(string key, string stylePath, string path, int startLod, int endLod,
            OnError errorHandler, IntPtr cancelToken);

        [DllImport("UtyMap.Shared")]
        private static extern void addDataInQuadKey(string key, string stylePath, string path, int tileX, int tileY, int lod,
            OnError errorHandler, IntPtr cancelToken);

        [DllImport("UtyMap.Shared")]
        private static extern void addDataInElement(string key, string stylePath, long id, double[] vertices, int vertexLength,
            string[] tags, int tagLength, int startLod, int endLod, OnError errorHandler, IntPtr cancelToken);

        [DllImport("UtyMap.Shared")]
        private static extern bool hasData(int tileX, int tileY, int levelOfDetails);

        #endregion

        #region Search API

        [DllImport("UtyMap.Shared")]
        private static extern void getDataByQuadKey(int tag, string stylePath, int tileX, int tileY, int levelOfDetails, int eleDataType,
            OnMeshBuilt meshBuiltHandler, OnElementLoaded elementLoadedHandler, OnError errorHandler, IntPtr cancelToken);

        [DllImport("UtyMap.Shared")]
        private static extern void getDataByText(int tag, string notTerms, string andTerms, string orTerms,
            double minLatitude, double minLogitude, double maxLatitude, double maxLogitude,
            int startLod, int endLod,
            OnElementLoaded elementLoadedHandler, OnError errorHandler, IntPtr cancelToken);

        [DllImport("UtyMap.Shared")]
        private static extern double getElevationByQuadKey(int tileX, int tileY, int levelOfDetails, int eleDataType, double latitude, double longitude);

        #endregion

        #endregion
    }
}
