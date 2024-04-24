import numpy as np
import awkward as ak

def normalize(array: ak.Array):
    flat_array = ak.flatten(array)
    return ak.to_numpy(ak.fill_none(flat_array, np.nan))