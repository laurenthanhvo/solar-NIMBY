import pandas as pd
import rasterio
import numpy as np
from rasterstats import zonal_stats

col_names = ["GHI", "Protected_Land", "Habitat", "Slope", "Population_Density", "Distance_to_Substation", "Land_Cover"]

def calculate_zonal_stats(tif_path, geodataframe, nodata_value ):
    with rasterio.open(tif_path) as src:
        affine = src.transform
        array = src.read(1)  # Read the first band
        array = np.where(np.isnan(array), nodata_value, array)  # Replace NaNs with nodata_value
        # Debugging: Check raster data and affine transformation
        print(f"Raster data shape: {array.shape}")
        print(f"Affine transformation: {affine}")
        # Check the CRS of the raster
        raster_crs = src.crs
        print(f"Raster CRS: {raster_crs}")
        if geodataframe.crs != raster_crs:
            geodataframe = geodataframe.to_crs(raster_crs)
            
        # filter out values that are less than 100
        array = np.where(array > 101, nodata_value, array)

    # Calculate zonal statistics
    stats = zonal_stats(geodataframe, array, affine=affine, stats="mean", nodata=nodata_value, all_touched=True)
    print(stats)
    # Extract mean values and add to GeoDataFrame
    mean_values = [stat['mean'] for stat in stats]
    return mean_values

def process_tif_files(tif_filepaths, bounding_box, nodata_value=-9999,bg=False):
    x = bounding_box.copy()

    # Initialize results DataFrame
    results = pd.DataFrame(index=x.index, columns=col_names)

    for tif_path, col_name in zip(tif_filepaths, col_names):
        print(f"Processing {tif_path} for {col_name}")

        # Calculate mean values using zonal stats
        mean_values = calculate_zonal_stats(tif_path, x, nodata_value)

        # Update results DataFrame
        results[col_name] = mean_values

    # Add county and state information
    results["County Name"] = bounding_box["County Name"]
    results["State"] = bounding_box["State"]
    if bg:
        results["GEOID"] = bounding_box["GEOID"]
        results["TRACTCE"] = bounding_box["TRACTCE"]
        results["BLKGRPCE"] = bounding_box["BLKGRPCE"]

    return results