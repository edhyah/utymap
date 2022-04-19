using System;
using System.Collections.Generic;
using UtyMap.Unity.Data;
using UtyMap.Unity.Infrastructure.Primitives;
using UtyRx;

namespace UtyMap.Unity.Geocoding
{
    /// <summary> Offline geocoder which uses local data only. </summary>
    public class UtyMapGeocoder : IGeocoder
    {
        private readonly IMapDataStore _dataStore;
        private readonly Range<int> _searchRange;
        private readonly string _notTerms = "";
        private readonly string _andTerms = "addr street housenumber";
        private readonly string _orTerms = "";

        private readonly List<UtyRx.IObserver<GeocoderResult>> _observers = new List<UtyRx.IObserver<GeocoderResult>>();
        private readonly IDisposable _subscription;

        /// <summary> Expected address tags. </summary>
        /// <see cref="http://wiki.openstreetmap.org/wiki/Key:addr"/>
        private readonly List<string> _addressTags = new List<string>()
        {
            "addr:country", "addr:city", "addr:suburb", "addr:state", "addr:province",
            "addr:district","addr:postcode", "addr:place", "addr:street", "addr:housenumber", "addr:name"
        };

        /// <summary> Creates geocoder which works with OSM data schema. </summary>
        [UtyDepend.Dependency]
        public UtyMapGeocoder(IMapDataStore dataStore)
        {
            _dataStore = dataStore;
            _searchRange = new Range<int>(16, 16);

           _subscription = _dataStore
                .Subscribe<Element>(ProcessResult);
        }

        /// <inheritdoc />
        public IDisposable Subscribe(UtyRx.IObserver<GeocoderResult> observer)
        {
            _observers.Add(observer);
            return Disposable.Empty;
        }

        /// <inheritdoc />
        void UtyRx.IObserver<UtyRx.Tuple<string, BoundingBox>>.OnCompleted()
        {
            _observers.Clear();
        }

        /// <inheritdoc />
        void UtyRx.IObserver<UtyRx.Tuple<string, BoundingBox>>.OnError(Exception error)
        {
            // Ignore
        }

        /// <inheritdoc />
        void UtyRx.IObserver<UtyRx.Tuple<GeoCoordinate, float>>.OnCompleted()
        {
            _observers.Clear();
        }

        /// <inheritdoc />
        void UtyRx.IObserver<UtyRx.Tuple<GeoCoordinate, float>>.OnError(Exception error)
        {
            // Ignore
        }

        /// <inheritdoc />
        public void OnNext(UtyRx.Tuple<string, BoundingBox> value)
        {
            _dataStore.OnNext(new MapQuery("", "", value.Item1, value.Item2, _searchRange));
        }

        /// <inheritdoc />
        public void OnNext(UtyRx.Tuple<GeoCoordinate, float> value)
        {
            _dataStore.OnNext(new MapQuery(_notTerms, _andTerms, _orTerms,
                BoundingBox.Create(value.Item1, value.Item2), _searchRange));
        }

        private void ProcessResult(Element element)
        {
            var address = GetAddress(element);

            _observers.ForEach(o => o.OnNext(new GeocoderResult()
            {
                Element = element,
                DisplayName = address,
            }));
        }

        /// <summary> Gets address string from element tags. </summary>
        private string GetAddress(Element element)
        {
            // TODO string manipulations can be optimized
            var tags = new List<string>();
            foreach (var tag in _addressTags)
            {
                if (element.Tags.ContainsKey(tag))
                    tags.Add(element.Tags[tag]);
            }

            return String.Join(", ", tags.ToArray());
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _observers.ForEach(o => o.OnCompleted());
            _observers.Clear();
            _subscription.Dispose();
        }
    }
}
