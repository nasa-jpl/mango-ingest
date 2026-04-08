def populate_dynamic_unit(row) -> str | None:
    """Returns the measurement unit based on the sensor type."""
    units = {
        "V": "V",
        "T": "degC",
        "A": "A"
    }
    return units.get(row.sensortype)
