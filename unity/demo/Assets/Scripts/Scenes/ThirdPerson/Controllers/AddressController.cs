using System;
using UnityEngine;
using UnityEngine.UI;
using UtyMap.Unity;
using UtyMap.Unity.Geocoding;
using UtyMap.Unity.Utils;

using UtyRx;

namespace Assets.Scripts.Scenes.ThirdPerson.Controllers
{
    /// <summary> Searches for address using cached locally data. </summary>
    internal sealed class AddressController: IDisposable
    {
        private const float SearchSize = 100;

        private readonly IGeocoder _geocoder;
        private readonly Text _text;
        private readonly IDisposable _subscription;

        private GeoCoordinate _coordinate;
        private Vector3 _position;

        private GeoCoordinate? _currentLocation;

        public AddressController(IGeocoder geocoder, Text text)
        {
            _geocoder = geocoder;
            _text = text;

            _subscription = _geocoder
                .ObserveOn(Scheduler.MainThread)
                .Subscribe(ProcessResult);
        }

        public void Update(Vector3 position, GeoCoordinate relativeNullPoint)
        {
            if (Vector3.Distance(position, _position) < 10)
                return;

            _position = position;
            _coordinate = GeoUtils.ToGeoCoordinate(relativeNullPoint, position.x, position.z);

            _geocoder.OnNext(new UtyRx.Tuple<GeoCoordinate, float>(_coordinate, SearchSize));
        }

        private void ProcessResult(GeocoderResult result)
        {
            var location = GetLocation(result.Element);

            var distanceToNewPlace = location.HasValue
                ? GeoUtils.Distance(location.Value, _coordinate)
                : 50; // NOTE give element some weight as we don't know its geometry

            var distanceToOldPlace = _currentLocation.HasValue
                ? GeoUtils.Distance(_currentLocation.Value, _coordinate)
                : float.MaxValue;

            if (location.HasValue && distanceToNewPlace <= distanceToOldPlace)
            {
                _text.text = result.DisplayName;
                _currentLocation = location.Value;
            }
        }

        /// <summary> Gets element location. </summary>
        private GeoCoordinate? GetLocation(Element element)
        {
            if (element.Geometry.Length == 1)
            {
                var location = element.Geometry[0];
                // NOTE Relations are not processed yet fully.
                if (Math.Abs(location.Latitude) < double.Epsilon &&
                    Math.Abs(location.Longitude) < double.Epsilon)
                    return null;
                return location;
            }

            double lat = 0, lon = 0;
            foreach (var coordinate in element.Geometry)
            {
                lat += coordinate.Latitude;
                lon += coordinate.Longitude;
            }

            return new GeoCoordinate(lat / element.Geometry.Length, lon / element.Geometry.Length);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _subscription.Dispose();
        }
    }
}
