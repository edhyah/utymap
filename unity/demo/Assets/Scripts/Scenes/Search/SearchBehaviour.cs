using System;
using System.Collections.Generic;
using System.Linq;
using Assets.Scripts.Core;
using Assets.Scripts.Core.Plugins;
using UnityEngine;
using UtyMap.Unity;
using UtyMap.Unity.Data;
using UtyMap.Unity.Infrastructure.Diagnostic;
using UtyMap.Unity.Infrastructure.Primitives;
using UtyMap.Unity.Utils;
using UtyRx;
using Component = UtyDepend.Component;
using CancellationToken = UtyMap.Unity.CancellationToken;

namespace Assets.Scripts.Scenes.Search
{
    /// <summary> Demonstrates how to use search. </summary>
    public class SearchBehaviour : MonoBehaviour
    {
        /// <summary> Path to map data on disk. </summary>
        private const string MapDataPath = @"../../../../core/test/test_assets/osm/berlin.osm.xml";

        /// <summary> Start coordinate: Unity's world zero point. </summary>
        private readonly GeoCoordinate _coordinate = new GeoCoordinate(ThirdPersonBehaviour.StartLatitude, ThirdPersonBehaviour.StartLongitude);
        /// <summary> Range to load. </summary>
        private readonly Range<int> _range = new Range<int>(16, 16);

        private CompositionRoot _compositionRoot;
        private IMapDataStore _mapDataStore;
        private ITrace _trace;

        void Start()
        {
            // init utymap library
            _compositionRoot = InitTask.Run((container, config) =>
            {
                container
                    // NOTE use another mapcss style
                    .Register(Component.For<Stylesheet>().Use<Stylesheet>(@"mapcss/default/index.mapcss"))
                    .Register(Component.For<MaterialProvider>().Use<MaterialProvider>())
                    .Register(Component.For<GameObjectBuilder>().Use<GameObjectBuilder>())
                    .RegisterInstance<IEnumerable<IElementBuilder>>(new List<IElementBuilder>());
            });

            // get trace for logging.
            _trace = _compositionRoot.GetService<ITrace>();

            // store map data store reference to member variable
            _mapDataStore = _compositionRoot.GetService<IMapDataStore>();

            // disable mesh caching to force import data into memory for every run
            _compositionRoot.GetService<IMapDataLibrary>().DisableCache();

            // import data into memory
            _mapDataStore.AddTo(
                    // define where geoindex is created (in memory, not persistent)
                    MapDataStorages.TransientStorageKey,
                    // path to map data
                    MapDataPath,
                    // stylesheet is used to import only used data and skip unused
                    _compositionRoot.GetService<Stylesheet>(),
                    // level of detail (zoom) for which map data should be imported
                    _range,
                    new CancellationToken())
                // start import and listen for events.
                .Subscribe(
                    // NOTE progress callback is ignored
                    (progress) => { },
                    // exception is reported
                    (exception) => _trace.Error("search", exception, "Cannot import map data"),
                    // once completed, load the corresponding tile
                    OnDataImported);
        }

        private void OnDataImported()
        {
            LoadTile();
            DoSearch();
        }

        /// <summary> Loads tile. </summary>
        private void LoadTile()
        {
            _mapDataStore.OnNext(new Tile(
                // create quadkey using coordinate and LOD
                GeoUtils.CreateQuadKey(_coordinate, _range.Maximum),
                // provide stylesheet
                _compositionRoot.GetService<Stylesheet>(),
                // use cartesian projection as we want to build flat world
                new CartesianProjection(_coordinate),
                // use flat elevation (all vertices have zero meters elevation)
                ElevationDataType.Flat,
                // parent for built game objects
                gameObject));
        }

        /// <summary> Performs search. </summary>
        private void DoSearch()
        {
            // analyze search results..
            _mapDataStore.Subscribe<Element>(element =>
            {
                _trace.Warn("Search", "Found element with id: {0}, tags: {1}",
                    element.Id.ToString(),
                    element.Tags
                           .Select(t => String.Format("{0}={1} ", t.Key, t.Value))
                           .Aggregate("", (ac, item) => ac + item));
            });

            // once tile is loaded..
            _mapDataStore.Subscribe<Tile>(_ =>
                {
                    string notTerms = "", andTerms = "Nordbahnhof tram stop", orTerms = "";
                    // ..and text search is performed
                    _mapDataStore.OnNext(new MapQuery(notTerms, andTerms, orTerms,
                        BoundingBox.Create(_coordinate, 5000), _range));
                });
            
        }
    }
}
