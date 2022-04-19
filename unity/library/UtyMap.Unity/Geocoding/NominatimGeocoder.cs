using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using UtyDepend;
using UtyDepend.Config;
using UtyMap.Unity.Infrastructure.Formats;
using UtyMap.Unity.Infrastructure.IO;
using UtyRx;

namespace UtyMap.Unity.Geocoding
{
    /// <summary> Online geocoder which uses remote osm nominatim server. </summary>
    internal class NominatimGeocoder : IGeocoder, IConfigurable
    {
        private const string DefaultSearchServer = @"http://nominatim.openstreetmap.org/search?";
        private const string DefaultReverseSearchServer = @"http://nominatim.openstreetmap.org/reverse?";

        // NOTE: nominatim wants user agent header.
        private readonly Dictionary<string, string> _headers = new Dictionary<string, string>()
        {
            { "User-Agent", "UtyMap" }
        };

        private readonly INetworkService _networkService;

        private string _searchPath;
        private string _reverseSearchPath;

        private List<UtyRx.IObserver<GeocoderResult>> _observers = new List<UtyRx.IObserver<GeocoderResult>>();

        [Dependency]
        public NominatimGeocoder(INetworkService networkService)
        {
            _networkService = networkService;
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
            var name = value.Item1;
            var area = value.Item2;

            var sb = new StringBuilder(128);
            sb.Append(_searchPath);
            if (area != null)
            {
                sb.Append(Uri.EscapeDataString(String.Format(CultureInfo.InvariantCulture,
                    "viewbox={0:f4},{1:f4},{2:f4},{3:f4}&",
                    area.MinPoint.Longitude,
                    area.MinPoint.Latitude,
                    area.MaxPoint.Longitude,
                    area.MaxPoint.Latitude)));
            }
            sb.AppendFormat("q={0}&format=json", Uri.EscapeDataString(name));

            _networkService.Get(sb.ToString(), _headers)
                .Take(1)
                .SelectMany(r => (
                    from JSONNode json in JSON.Parse(r).AsArray
                    select ParseGeocoderResult(json)))
                .Subscribe(r => _observers.ForEach(o => o.OnNext(r)));
        }

        /// <inheritdoc />
        public void OnNext(UtyRx.Tuple<GeoCoordinate, float> value)
        {
            var coordinate = value.Item1;
            var url = String.Format("{0}format=json&lat={1}&lon={2}",
                _reverseSearchPath, coordinate.Latitude, coordinate.Longitude);

            _networkService
                .Get(url, _headers)
                .Take(1)
                .Select(r => ParseGeocoderResult(JSON.Parse(r)))
                .Subscribe(r => _observers.ForEach(o => o.OnNext(r)));
        }

        private GeocoderResult ParseGeocoderResult(JSONNode resultNode)
        {
            BoundingBox bbox = null;
            string[] bboxArray = resultNode["boundingbox"].Value.Split(',');
            if (bboxArray.Length == 4)
            {
                bbox = new BoundingBox(ParseGeoCoordinate(bboxArray[0], bboxArray[2]),
                    ParseGeoCoordinate(bboxArray[1], bboxArray[3]));
            }

            var id = long.Parse(resultNode["osm_id"].Value);
            var coordinate = ParseGeoCoordinate(resultNode["lat"].Value, resultNode["lon"].Value);

            return new GeocoderResult()
            {
                Element = new Element(id, new [] { coordinate }, null, null, null),
                DisplayName = resultNode["display_name"].Value,
                BoundingBox = bbox,
            };
        }

        private static GeoCoordinate ParseGeoCoordinate(string latStr, string lonStr)
        {
            double latitude, longitude;
            if (double.TryParse(latStr, out latitude) && double.TryParse(lonStr, out longitude))
                return new GeoCoordinate(latitude, longitude);
            return default(GeoCoordinate);
        }

        /// <inheritdoc />
        public void Configure(IConfigSection configSection)
        {
            _searchPath = configSection.GetString("geocoding", DefaultSearchServer);
            _reverseSearchPath = configSection.GetString("reverse_geocoding", DefaultReverseSearchServer);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _observers.ForEach(o => o.OnCompleted());
            _observers.Clear();
        }
    }
}
