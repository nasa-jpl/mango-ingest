import numpy as np

class Geolocation:
    """
    This class provides methods for adding geolocation field to a dataframe.
    The data frame must have columns xpos, ypos and zpos,
    that hold coordinates in Earth-fixed coordinate system
    """

    @classmethod
    def append_location_fields(cls, df):
        """
        Append a location and orbit_direction (ascending or descending flag)
        columns to the data frame.

        Parameters
        ----------
        df -  pd.DataFrame
        """
        df['location'] = df.apply(cls.populate_location, axis=1, result_type='expand')
        # TODO: confirm that we can use ZPOS instead on lat to determine orbit direction
        # It is better to use zpos because it is already available in the dataframe
        df['orbit_direction'] = cls.get_orbit_direction(df['zpos'])


    @classmethod
    def populate_location(cls, row)-> str:
        lat, lon = cls.computeLatLon(row.xpos, row.ypos, row.zpos)
        # returns a string representation of POINT in WKT format
        return f'POINT( {lon}  {lat})'

    @classmethod
    def get_orbit_direction(cls, coord_array) -> np.array:
        """
        Determine orbit direction (ascending or descending) based of values in input coord_array:
        If next value is bigger or equal to the current value, the direction is ascending (‘A’),
        otherwise descending (‘D’)

        Parameters
        ----------
        coord_array -  np.array of doubles representing vertical coordinates

        Return
        ----------
        np.array of characters, 'A' for ascending, 'D' for descending

        """
        orbit_direction= np.where(np.diff(coord_array) <= 0, 'A', 'D')

        # We can't calculate difference for the last element,
        # so assume that the direction of orbit for the last point
        # is the same as for the previous point.
        # Append one value to the end, the same as the last one

        return np.append(orbit_direction, orbit_direction[-1])

    @classmethod
    def computeLatLon( cls, x, y, z ):
        """
        X/Y/Z are the positions from the GNV files. Longitude is imbedded in the code.
        The output here is geodetic latitude;
        however, if you only want geocentric latitude, the entire while loop is unnecessary.
        Note that np here is numpy.
        """

        ae = 6378136.3
        flat = 1.0 / 298.25645
        e2 = 2*flat - flat*flat
        tol = 1e-10
        diff = 2*tol

        lat = np.arcsin( z / np.sqrt( x*x + y*y + z*z ) )

        rxy = np.sqrt( x*x + y*y )
        lon = np.arctan2( y, x )
        while ( diff > tol ):
            C = ae / np.sqrt( 1 - e2*(np.sin(lat)**2) )
            latNew = np.arctan2( z + C * e2 * np.sin( lat ), rxy )
            diff = latNew - lat
            lat = latNew
        # to convert from rad to degree, multiply by 180/numpy.pi
        return lat * 57.295779513, lon  * 57.295779513
