﻿using System;
using System.Collections.Generic;
using System.Threading;
using UtyMap.Unity.Infrastructure.Config;
using UtyMap.Unity.Infrastructure.Diagnostic;
using UtyMap.Unity.Infrastructure.IO;
using UtyDepend;
using UtyDepend.Config;
using UtyMap.Unity.Data;
using UtyRx;

namespace UtyMap.Unity.Tests.Helpers
{
    internal static class TestHelper
    {
        #region Constants values

        public const string IntegrationTestCategory = "Integration";

        public static GeoCoordinate WorldZeroPoint = new GeoCoordinate(52.5317429, 13.3871987);

        public const string TestAssetsFolder = @"../../../../../core/test/test_assets/";
        
        public const string BerlinXmlData = TestAssetsFolder + @"osm/berlin.osm.xml";
        public const string BerlinPbfData = TestAssetsFolder + @"osm/berlin.osm.pbf";
        public const string MoscowJsonData = TestAssetsFolder + @"osm/moscow.osm.json";
        public const string DefaultMapCss = @"../../../../demo/Assets/StreamingAssets/mapcss/default/index.mapcss";

        public const string TransientStorageKey = "primary";
        public const string PersistentStorageKey = "secondary";

        private const string IndexPath = @"../../../../demo/Assets/StreamingAssets/index";

        #endregion

        public static CompositionRoot GetCompositionRoot(GeoCoordinate worldZeroPoint)
        {
            return GetCompositionRoot(worldZeroPoint, (container, section) => { });
        }

        public static CompositionRoot GetCompositionRoot(GeoCoordinate worldZeroPoint,
            Action<IContainer, IConfigSection> action)
        {
            // create default container which should not be exposed outside
            // to avoid Service Locator pattern.
            IContainer container = new Container();

            // create default application configuration
            var config = ConfigBuilder.GetDefault()
                .SetIndex(IndexPath)
                .Build();

            // initialize services
            var root = new CompositionRoot(container, config)
                .RegisterAction((c, _) => c.Register(Component.For<ITrace>().Use<ConsoleTrace>()))
                .RegisterAction((c, _) => c.Register(Component.For<IPathResolver>().Use<TestPathResolver>()))
                .RegisterAction((c, _) => c.Register(Component.For<Stylesheet>().Use<Stylesheet>(DefaultMapCss)))
                .RegisterAction((c, _) => c.Register(Component.For<IProjection>().Use<CartesianProjection>(worldZeroPoint)))
                .RegisterAction(action)
                .Setup();

            // Register default data stores to simplify test setup. The order is important
            var mapDataStore = root.GetService<IMapDataStore>();
            mapDataStore.Register(TransientStorageKey);
            mapDataStore.Register(PersistentStorageKey, IndexPath + "/data");

            return root;
        }

        public static MapData GetResultSync(this IMapDataStore store, Tile tile, int waitTimeInSeconds = 10)
        {
            MapData mapData = null;
            var manualResetEvent = new ManualResetEvent(false);
            store
                .ObserveOn<Tile>(Scheduler.CurrentThread)
                .SubscribeOn(Scheduler.CurrentThread)
                .Subscribe(_ => manualResetEvent.Set());
            store
                .ObserveOn<MapData>(Scheduler.CurrentThread)
                .SubscribeOn(Scheduler.CurrentThread)
                .Subscribe(r => mapData = r);
            store.OnNext(tile);
            manualResetEvent.WaitOne(TimeSpan.FromSeconds(waitTimeInSeconds));
            return mapData;
        }

        public static UtyRx.Tuple<Tile, string> GetResultSync(this ISubject<Tile, UtyRx.Tuple<Tile, string>> source, Tile tile)
        {
            var result = default(UtyRx.Tuple<Tile, string>);
            var manualResetEvent = new ManualResetEvent(false);
            source
                .Subscribe(r =>
                {
                    result = r;
                    manualResetEvent.Set();
                });

            source.OnNext(tile);

            manualResetEvent.WaitOne(TimeSpan.FromSeconds(5));
            return result;
        }

        public static Element GetResultSync(this IMapDataStore store, MapQuery query, int waitTimeInSeconds = 10)
        {
            Element element = null;
            var manualResetEvent = new ManualResetEvent(false);
            store
                .ObserveOn<Element>(Scheduler.CurrentThread)
                .SubscribeOn(Scheduler.CurrentThread)
                .Subscribe(r =>
                {
                    element = r;
                    manualResetEvent.Set();
                });
            store.OnNext(query);
            manualResetEvent.WaitOne(TimeSpan.FromSeconds(waitTimeInSeconds));
            return element;
        }
    }
}
