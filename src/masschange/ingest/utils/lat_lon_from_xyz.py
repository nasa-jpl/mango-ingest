import numpy as np
"""
X/Y/Z are the positions from the GNV files. Longitude is imbedded in the code. The output here is geodetic latitude; 
however, if you only want geocentric latitude, the entire while loop is unnecessary. Note that np here is numpy.
"""
def computeLatLon( x, y, z ):

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
    return lat * 57.2958, lon  * 57.2958
