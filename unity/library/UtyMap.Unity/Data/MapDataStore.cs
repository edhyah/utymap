using System;
using System.Collections.Generic;
using System.Linq;
using UtyDepend;
using UtyDepend.Config;
using UtyMap.Unity.Infrastructure.Primitives;
using UtyRx;

namespace UtyMap.Unity.Data
{
    /// <summary> Defines behavior of class responsible of mapdata processing. </summary>
    public interface IMapDataStore : UtyRx.IObserver<Tile>, UtyRx.IObserver<MapQuery>,
        UtyRx.IObservable<MapData>, UtyRx.IObservable<Tile>, UtyRx.IObservable<Element>
    {
        /// <summary> Registers in-memory data storage with given key. </summary>
        /// <param name="storageKey"> Storage key.</param>
        void Register(string storageKey);

        /// <summary> Registers persistent data storage with given key. </summary>
        /// <param name="storageKey"> Storage key.</param>
        /// <param name="dataPath"> Data path. </param>
        void Register(string storageKey, string dataPath);

        /// <summary> Adds mapdata to the specific data storage. </summary>
        /// <param name="storageKey"> Storage key. </param>
        /// <param name="dataPath"> Path to mapdata. </param>
        /// <param name="stylesheet"> Stylesheet which to use during import. </param>
        /// <param name="levelOfDetails"> Which level of details to use. </param>
        /// <param name="cancellationToken"> Cancellation token. </param>
        /// <returns> Returns progress status. </returns>
        UtyRx.IObservable<int> AddTo(string storageKey, string dataPath, Stylesheet stylesheet, Range<int> levelOfDetails, CancellationToken cancellationToken);

        /// <summary> Adds mapdata to the specific data storage. </summary>
        /// <param name="storageKey"> Storage key. </param>
        /// <param name="dataPath"> Path to mapdata. </param>
        /// <param name="stylesheet"> Stylesheet which to use during import. </param>
        /// <param name="quadKey"> QuadKey to add. </param>
        /// <param name="cancellationToken"> Cancellation token. </param>
        /// <returns> Returns progress status. </returns>
        UtyRx.IObservable<int> AddTo(string storageKey, string dataPath, Stylesheet stylesheet, QuadKey quadKey, CancellationToken cancellationToken);
    }

    /// <summary> Default implementation of map data store. </summary>
    internal class MapDataStore : IMapDataStore, IDisposable, IConfigurable
    {
        private readonly IMapDataProvider _mapDataProvider;
        private readonly IMapDataLibrary _mapDataLibrary;

        private readonly List<string> _storageKeys = new List<string>();
        private readonly List<UtyRx.IObserver<MapData>> _dataObservers = new List<UtyRx.IObserver<MapData>>();
        private readonly List<UtyRx.IObserver<Tile>> _tileObservers = new List<UtyRx.IObserver<Tile>>();
        private readonly List<UtyRx.IObserver<Element>> _elementObservers = new List<UtyRx.IObserver<Element>>();

        [Dependency]
        public MapDataStore(IMapDataProvider mapDataProvider, IMapDataLibrary mapDataLibrary)
        {
            _mapDataLibrary = mapDataLibrary;

            _mapDataProvider = mapDataProvider;
            _mapDataProvider
                .ObserveOn(Scheduler.ThreadPool)
                .Subscribe(value =>
                {
                    // We have map data in store.
                    if (String.IsNullOrEmpty(value.Item2))
                        _mapDataLibrary
                            .Get(value.Item1, _dataObservers)
                            .Subscribe(_ => _tileObservers.ForEach(t => t.OnNext(value.Item1)));
                    else
                        // NOTE store data in the first registered store
                        AddTo(_storageKeys.First(), value.Item2, value.Item1.Stylesheet, value.Item1.QuadKey, value.Item1.CancelationToken)
                            .Subscribe(progress => { }, () => _mapDataLibrary.Get(value.Item1, _dataObservers));
                });
        }

        #region Interface implementations

        /// <inheritdoc />
        public void Register(string storageKey)
        {
            _storageKeys.Add(storageKey);
            _mapDataLibrary.Register(storageKey);
        }

        /// <inheritdoc />
        public void Register(string storageKey, string dataPath)
        {
            _storageKeys.Add(storageKey);
            _mapDataLibrary.Register(storageKey, dataPath);
        }

        /// <inheritdoc />
        public UtyRx.IObservable<int> AddTo(string storageKey, string dataPath, Stylesheet stylesheet, Range<int> levelOfDetails, CancellationToken cancellationToken)
        {
            return _mapDataLibrary.AddTo(storageKey, dataPath, stylesheet, levelOfDetails, cancellationToken);
        }

        /// <inheritdoc />
        public UtyRx.IObservable<int> AddTo(string storageKey, string dataPath, Stylesheet stylesheet, QuadKey quadKey, CancellationToken cancellationToken)
        {
            return _mapDataLibrary.Exists(quadKey)
                ? Observable.Return<int>(100)
                : _mapDataLibrary.AddTo(storageKey, dataPath, stylesheet, quadKey, cancellationToken);
        }

        /// <inheritdoc />
        void UtyRx.IObserver<MapQuery>.OnCompleted()
        {
            _dataObservers.ForEach(o => o.OnCompleted());
        }

        /// <inheritdoc />
        void UtyRx.IObserver<MapQuery>.OnError(Exception error)
        {
            _dataObservers.ForEach(o => o.OnError(error));
        }

         /// <inheritdoc />
         void UtyRx.IObserver<Tile>.OnCompleted()
         {
             _tileObservers.ForEach(o => o.OnCompleted());
         }

         /// <inheritdoc />
         void UtyRx.IObserver<Tile>.OnError(Exception error)
         {
             _tileObservers.ForEach(o => o.OnError(error));
         }

        /// <summary> Triggers loading data for given tile. </summary>
        public void OnNext(Tile tile)
        {
            _mapDataProvider.OnNext(tile);
        }

        /// <summary> Triggers search of elements matching query query. </summary>
        public void OnNext(MapQuery value)
        {
            _mapDataLibrary.Get(value, _elementObservers);
        }

        /// <summary> Subscribes on mesh/element data loaded events. </summary>
        public IDisposable Subscribe(UtyRx.IObserver<MapData> observer)
        {
            _dataObservers.Add(observer);
            return Disposable.Empty;
        }

        /// <summary> Subscribes on tile fully load event. </summary>
        public IDisposable Subscribe(UtyRx.IObserver<Tile> observer)
        {
            _tileObservers.Add(observer);
            return Disposable.Empty;
        }

        /// <summary> Subscribes on element search event. </summary>
        public IDisposable Subscribe(UtyRx.IObserver<Element> observer)
        {
            _elementObservers.Add(observer);
            return Disposable.Empty;
        }

        /// <inheritdoc />
        public void Configure(IConfigSection configSection)
        {
            _mapDataLibrary.Configure(configSection.GetString("data/index"));
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _mapDataLibrary.Dispose();
        }

        #endregion
    }
}
