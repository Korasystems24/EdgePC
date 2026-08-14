class RadarError(Exception): ...


class ClientNotInitialized(RadarError): ...


class RadarCatalogNotReady(RadarError):
    """GetParkNames/GetVesselNames вернули пустой список — LIMACO ещё поднимается."""


class RadarSelectionError(RadarError): ...


class ValueRadarError(RadarError, ValueError): ...
