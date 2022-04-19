using System;
using UtyRx;

namespace UtyMap.Unity.Geocoding
{
    /// <summary> Defines geocoder API (including reverse). </summary>
    public interface IGeocoder :
        UtyRx.IObservable<GeocoderResult>,
        UtyRx.IObserver<UtyRx.Tuple<string, BoundingBox>>, // place description and bbox restriction
        UtyRx.IObserver<UtyRx.Tuple<GeoCoordinate, float>>, // place coordinate and surrounding bbox size restriction (in meters)
        IDisposable
    {
    }

    /// <summary> Represents geocoding results. </summary>
    public struct GeocoderResult
    {
        /// <summary> Gets or sets element.</summary>
        public Element Element;

        /// <summary> Gets or sets element's bounding box.</summary>
        public BoundingBox BoundingBox;

        /// <summary> Gets or sets formatted name of search result.</summary>
        public string DisplayName;
    }
}
