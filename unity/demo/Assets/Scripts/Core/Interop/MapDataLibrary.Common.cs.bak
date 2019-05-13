using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Assets.Scripts.Core.Plugins;
using UtyDepend;
using UtyMap.Unity;
using UtyMap.Unity.Data;
using UtyMap.Unity.Infrastructure.Diagnostic;
using UtyMap.Unity.Infrastructure.IO;
using UtyMap.Unity.Infrastructure.Primitives;
using UtyRx;

using CancellationToken = UtyMap.Unity.CancellationToken;

namespace Assets.Scripts.Core.Interop
{
    internal sealed partial class MapDataLibrary : IMapDataLibrary
    {
        private const string TraceCategory = "library";

        private static readonly object __lockObj = new object();
        private volatile bool _isConfigured;
        private readonly MaterialProvider _materialProvider;
        private readonly IPathResolver _pathResolver;

        private HashSet<string> _stylePaths = new HashSet<string>();

        [Dependency]
        public MapDataLibrary(MaterialProvider materialProvider, IPathResolver pathResolver, ITrace trace)
        {
            _materialProvider = materialProvider;
            _pathResolver = pathResolver;
            _trace = trace;
        }

        /// <inheritdoc />
        public void Configure(string indexPath)
        {
            lock (__lockObj)
            {
                indexPath = _pathResolver.Resolve(indexPath);

                _trace.Debug(TraceCategory, "Configure with {0}", indexPath);
                
                if (!Directory.Exists(indexPath))
                    throw new DirectoryNotFoundException(String.Format("Cannot find {0}", indexPath));

                if (_isConfigured) return;

                connect(indexPath, OnErrorHandler);

                // NOTE Enable mesh caching mechanism for speed up tile loading.
                enableMeshCache(1);
                
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

        public void Register(string storageKey)
        {
            registerInMemoryStore(storageKey);
        }

        /// <inheritdoc />
        public void Register(string storageKey, string indexPath)
        {
            registerPersistentStore(storageKey, _pathResolver.Resolve(indexPath), OnCreateDirectory);
        }

        /// <inheritdoc />
        public IObservable<int> AddTo(string storageKey, string path, Stylesheet stylesheet, Range<int> levelOfDetails, CancellationToken cancellationToken)
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
        public IObservable<int> AddTo(string storageKey, string path, Stylesheet stylesheet, QuadKey quadKey, CancellationToken cancellationToken)
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
        public IObservable<int> AddTo(string storageKey, Element element, Stylesheet stylesheet, Range<int> levelOfDetails, CancellationToken cancellationToken)
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

        private IObservable<int> Get(Tile tile, int tag, OnMeshBuilt meshBuiltHandler, OnElementLoaded elementLoadedHandler, OnError errorHandler)
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

        private IObservable<int> Get(MapQuery query, int tag, OnElementLoaded elementLoadedHandler, OnError errorHandler)
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

        private string RegisterStylesheet(Stylesheet stylesheet)
        {
            var stylePath = _pathResolver.Resolve(stylesheet.Path);

            if (_stylePaths.Contains(stylePath))
                return stylePath;

            _stylePaths.Add(stylePath);
            registerStylesheet(stylePath, OnCreateDirectory);

            return stylePath;
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
